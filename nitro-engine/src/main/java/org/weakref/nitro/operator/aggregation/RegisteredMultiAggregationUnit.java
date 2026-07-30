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
package org.weakref.nitro.operator.aggregation;

import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.MultiAggregationImplementation;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Engine-owned lowering from a registry multi-aggregate protocol to one physical execution unit.
 */
public final class RegisteredMultiAggregationUnit
        implements PhysicalAggregationUnit
{
    private final MultiAggregationImplementation implementation;
    private final List<OutputMode> outputModes;
    private final int[] inputColumns;
    private final int filterInputColumn;

    public RegisteredMultiAggregationUnit(
            MultiAggregationImplementation implementation,
            List<OutputMode> outputModes,
            int[] inputColumns)
    {
        this(implementation, outputModes, inputColumns, -1);
    }

    public RegisteredMultiAggregationUnit(
            MultiAggregationImplementation implementation,
            List<OutputMode> outputModes,
            int[] inputColumns,
            int filterInputColumn)
    {
        this.implementation = requireNonNull(implementation, "implementation is null");
        this.outputModes = requireNonNull(outputModes, "outputModes is null").stream()
                .map(mode -> requireNonNull(mode, "outputModes contains null"))
                .toList();
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null").clone();
        this.filterInputColumn = filterInputColumn;
        if (filterInputColumn < -1) {
            throw new IllegalArgumentException("filter input column is less than -1");
        }
        if (implementation.outputCount() < 1) {
            throw new IllegalArgumentException("implementation has no outputs");
        }
        if (this.outputModes.size() != implementation.outputCount()) {
            throw new IllegalArgumentException("outputModes size does not match implementation outputs");
        }
        if (Arrays.stream(this.inputColumns).anyMatch(column -> column < 0)) {
            throw new IllegalArgumentException("input column is negative");
        }
    }

    @Override
    public int filterInputColumn()
    {
        return filterInputColumn;
    }

    @Override
    public int outputCount()
    {
        return outputModes.size();
    }

    @Override
    public int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        return implementation.stateCapacity(requiredGroups, defaultCapacity);
    }

    @Override
    public Object allocate(AggregationExecutionContext context, int size)
    {
        return implementation.allocate(
                new AggregationExecution(context.allocator(), context.allocationContext(), context.inputSchema()),
                size);
    }

    @Override
    public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int size)
    {
        return implementation.grow(allocator, allocationContext, state, size);
    }

    @Override
    public void initialize(Object state, int offset, int length)
    {
        implementation.initialize(state, offset, length);
    }

    @Override
    public void accumulate(Object state, int group, Mask mask, StreamAccessor streams)
    {
        implementation.addRawInput(state, group, mask, input(streams));
    }

    @Override
    public void accumulate(Object state, Vector groups, Mask mask, StreamAccessor streams)
    {
        implementation.addRawInput(state, groups, mask, input(streams));
    }

    @Override
    public Streams result(
            int output,
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        requireOutput(output);
        if (outputModes.get(output) == OutputMode.INTERMEDIATE) {
            return implementation.intermediate(output, maxGroup, state, mask, existing, allocator, allocationContext);
        }
        return implementation.result(output, maxGroup, state, mask, existing, allocator, allocationContext);
    }

    @Override
    public Streams result(
            int output,
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        requireOutput(output);
        if (outputModes.get(output) == OutputMode.INTERMEDIATE) {
            return implementation.intermediate(output, maxGroup, state, existing, allocator, allocationContext);
        }
        return implementation.result(output, maxGroup, state, existing, allocator, allocationContext);
    }

    @Override
    public Streams copyResultPosition(
            int output,
            int group,
            int maxGroup,
            Object state,
            Streams existing,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        requireOutput(output);
        if (outputModes.get(output) == OutputMode.INTERMEDIATE) {
            return implementation.copyIntermediatePosition(
                    output, group, maxGroup, state, existing, outputPosition, size, allocator, allocationContext);
        }
        return implementation.copyResultPosition(
                output, group, maxGroup, state, existing, outputPosition, size, allocator, allocationContext);
    }

    private AggregationInput input(StreamAccessor streams)
    {
        return (input, stream) -> {
            if (input < 0 || input >= inputColumns.length) {
                throw new IndexOutOfBoundsException("multi-aggregate input: " + input);
            }
            return streams.stream(inputColumns[input], stream);
        };
    }

    private void requireOutput(int output)
    {
        if (output < 0 || output >= outputModes.size()) {
            throw new IndexOutOfBoundsException("multi-aggregate result: " + output);
        }
    }

    public enum OutputMode
    {
        INTERMEDIATE,
        FINAL
    }
}
