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
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationDomain;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.ValueDemand;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Engine-owned lowering from a registry aggregate protocol to one physical execution unit.
 */
public class RegisteredAggregationUnit
        implements PhysicalAggregationUnit
{
    private final AggregationImplementation implementation;
    private final InputMode inputMode;
    private final OutputMode outputMode;
    private final int[] inputColumns;
    private final int filterInputColumn;

    public RegisteredAggregationUnit(
            AggregationImplementation implementation,
            InputMode inputMode,
            OutputMode outputMode,
            int[] inputColumns)
    {
        this(implementation, inputMode, outputMode, inputColumns, -1);
    }

    @Override
    public PhysicalAggregationUnit physicalIntermediateOutput()
    {
        if (outputMode != OutputMode.INTERMEDIATE) {
            return this;
        }
        return new RegisteredAggregationUnit(
                implementation.physicalIntermediateOutput(),
                inputMode,
                outputMode,
                inputColumns,
                filterInputColumn);
    }

    protected final AggregationImplementation implementation()
    {
        return implementation;
    }

    protected final InputMode inputMode()
    {
        return inputMode;
    }

    protected final OutputMode outputMode()
    {
        return outputMode;
    }

    protected final int[] inputColumns()
    {
        return inputColumns.clone();
    }

    protected final int filterColumn()
    {
        return filterInputColumn;
    }

    public RegisteredAggregationUnit(
            AggregationImplementation implementation,
            InputMode inputMode,
            OutputMode outputMode,
            int[] inputColumns,
            int filterInputColumn)
    {
        this.implementation = requireNonNull(implementation, "implementation is null");
        this.inputMode = requireNonNull(inputMode, "inputMode is null");
        this.outputMode = requireNonNull(outputMode, "outputMode is null");
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null").clone();
        this.filterInputColumn = filterInputColumn;
        if (filterInputColumn < -1) {
            throw new IllegalArgumentException("filter input column is less than -1");
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
        return 1;
    }

    @Override
    public Map<Integer, ValueDemand> inputValueDemands()
    {
        HashMap<Integer, ValueDemand> demands = new HashMap<>();
        for (int input = 0; input < inputColumns.length; input++) {
            ValueDemand demand = inputMode == InputMode.RAW
                    ? requireNonNull(implementation.rawInputValueDemand(input), "raw input value demand is null")
                    : ValueDemand.FULL;
            demands.merge(inputColumns[input], demand, ValueDemand::merge);
        }
        if (filterInputColumn >= 0) {
            demands.merge(filterInputColumn, ValueDemand.FULL, ValueDemand::merge);
        }
        return Map.copyOf(demands);
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
        if (inputMode == InputMode.RAW) {
            implementation.addRawInput(state, group, mask, input(streams));
        }
        else {
            implementation.addIntermediate(state, group, mask, input(streams));
        }
    }

    @Override
    public void accumulate(Object state, Vector groups, Mask mask, StreamAccessor streams)
    {
        if (inputMode == InputMode.RAW) {
            implementation.addRawInput(state, groups, mask, input(streams));
        }
        else {
            implementation.addIntermediate(state, groups, mask, input(streams));
        }
    }

    @Override
    public boolean supportsEncodedGroupedInput()
    {
        return implementation.supportsEncodedGroupedInput();
    }

    @Override
    public boolean supportsGroupedDomainInput(StreamAccessor streams)
    {
        return inputMode == InputMode.RAW && implementation.supportsRawGroupedDomainInput(input(streams));
    }

    @Override
    public boolean supportsGroupedDomainInput(GroupedAggregationDomain domain, StreamAccessor streams)
    {
        return inputMode == InputMode.RAW && implementation.supportsRawGroupedDomainInput(domain, input(streams));
    }

    @Override
    public boolean supportsGroupedDomainInput(DictionaryVector rowMapping, StreamAccessor streams)
    {
        return inputMode == InputMode.RAW && implementation.supportsRawGroupedDomainInput(rowMapping, input(streams));
    }

    @Override
    public boolean requiresGroupedDomainRepresentatives(DictionaryVector rowMapping, StreamAccessor streams)
    {
        return inputMode == InputMode.RAW && implementation.requiresRawGroupedDomainRepresentatives(rowMapping, input(streams));
    }

    @Override
    public void accumulateGroupedDomain(Object state, GroupedAggregationDomain domain, StreamAccessor streams)
    {
        if (!supportsGroupedDomainInput(domain, streams)) {
            throw new UnsupportedOperationException("grouped domain input is not supported");
        }
        implementation.addRawGroupedDomainInput(state, domain, input(streams));
    }

    @Override
    public boolean supportsInitialInput()
    {
        return inputMode == InputMode.RAW &&
                outputMode == OutputMode.INTERMEDIATE &&
                filterInputColumn < 0 &&
                implementation.supportsInitialRawIntermediate();
    }

    @Override
    public Streams initialInput(
            int output,
            Mask mask,
            StreamAccessor streams,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        requireOnlyOutput(output);
        if (!supportsInitialInput()) {
            throw new UnsupportedOperationException("direct initial input is not supported");
        }
        return requireNonNull(
                implementation.initialRawIntermediate(mask, input(streams), allocator, allocationContext),
                "initial raw intermediate result is null");
    }

    @Override
    public boolean supportsPositionPreservingInitialInput()
    {
        return supportsInitialInput() && implementation.supportsPositionPreservingInitialRawIntermediate();
    }

    @Override
    public Streams positionPreservingInitialInput(
            int output,
            Mask mask,
            StreamAccessor streams,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        requireOnlyOutput(output);
        if (!supportsPositionPreservingInitialInput()) {
            throw new UnsupportedOperationException("position-preserving initial input is not supported");
        }
        return requireNonNull(
                implementation.positionPreservingInitialRawIntermediate(mask, input(streams), allocator, allocationContext),
                "position-preserving initial raw intermediate result is null");
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
        requireOnlyOutput(output);
        if (outputMode == OutputMode.INTERMEDIATE) {
            return implementation.intermediate(maxGroup, state, mask, existing, allocator, allocationContext);
        }
        return implementation.result(maxGroup, state, mask, existing, allocator, allocationContext);
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
        requireOnlyOutput(output);
        if (outputMode == OutputMode.INTERMEDIATE) {
            return implementation.intermediate(maxGroup, state, existing, allocator, allocationContext);
        }
        return implementation.result(maxGroup, state, existing, allocator, allocationContext);
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
        requireOnlyOutput(output);
        if (outputMode == OutputMode.INTERMEDIATE) {
            return implementation.copyIntermediatePosition(
                    group, maxGroup, state, existing, outputPosition, size, allocator, allocationContext);
        }
        return implementation.copyResultPosition(
                group, maxGroup, state, existing, outputPosition, size, allocator, allocationContext);
    }

    @Override
    public Streams copyResultRange(
            int output,
            int groupStart,
            int groupCount,
            int maxGroup,
            Object state,
            Streams existing,
            int outputStart,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        requireOnlyOutput(output);
        if (outputMode == OutputMode.INTERMEDIATE) {
            return implementation.copyIntermediateRange(
                    groupStart, groupCount, maxGroup, state, existing, outputStart, size, allocator, allocationContext);
        }
        return implementation.copyResultRange(
                groupStart, groupCount, maxGroup, state, existing, outputStart, size, allocator, allocationContext);
    }

    private AggregationInput input(StreamAccessor streams)
    {
        return (input, stream) -> {
            if (input < 0 || input >= inputColumns.length) {
                throw new IndexOutOfBoundsException("aggregate input: " + input);
            }
            return streams.stream(inputColumns[input], stream);
        };
    }

    private static void requireOnlyOutput(int output)
    {
        if (output != 0) {
            throw new IndexOutOfBoundsException("single-output registered aggregate result: " + output);
        }
    }

    @Override
    public String toString()
    {
        return "%s[%s -> %s]".formatted(implementation.getClass().getName(), inputMode, outputMode);
    }

    public enum InputMode
    {
        RAW,
        INTERMEDIATE
    }

    public enum OutputMode
    {
        INTERMEDIATE,
        FINAL
    }
}
