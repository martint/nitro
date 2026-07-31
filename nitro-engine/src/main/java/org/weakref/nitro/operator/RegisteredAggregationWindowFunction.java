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
package org.weakref.nitro.operator;

import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.AggregationPositionAccumulator;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Adapts one dynamically resolved aggregate to an exact running or whole-partition window.
 *
 * <p>The aggregate implementation owns all function and type semantics. This adapter owns only
 * the one-group window lifecycle, physical input mapping, and generic result-stream placement.
 */
public final class RegisteredAggregationWindowFunction
        implements RunningWindowFunction
{
    private final AggregationImplementation implementation;
    private final Schema inputSchema;
    private final int[] inputColumns;
    private final Frame frame;
    private final int[] activePosition = new int[1];

    private Object state;
    private Streams result;
    private Streams[] boundSourceColumns;
    private AggregationInput boundInput;
    private AggregationPositionAccumulator boundPositionAccumulator;

    public RegisteredAggregationWindowFunction(
            AggregationImplementation implementation,
            Schema inputSchema,
            int[] inputColumns,
            Frame frame)
    {
        this.implementation = requireNonNull(implementation, "implementation is null");
        this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null").clone();
        this.frame = requireNonNull(frame, "frame is null");
        if (Arrays.stream(this.inputColumns).anyMatch(column -> column < 0 || column >= inputSchema.size())) {
            throw new IllegalArgumentException("aggregate input column is outside the input schema");
        }
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        if (state != null) {
            throw new IllegalStateException("aggregate window output is already initialized");
        }
        state = implementation.allocate(
                new AggregationExecution(allocator, allocationContext, inputSchema),
                1);
        implementation.initialize(state, 0, 1);
        result = implementation.result(0, state, Streams.empty(), allocator, allocationContext);

        Streams.Builder output = Streams.builder();
        for (Stream stream : result.streams()) {
            output.put(stream, result.get(stream).emptyLike(allocator, allocationContext));
        }
        return output.build();
    }

    @Override
    public void reset()
    {
        requireInitialized();
        implementation.initialize(state, 0, 1);
    }

    @Override
    public Streams append(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            Streams[] sourceColumns,
            int inputPosition,
            int outputPosition,
            int outputSize)
    {
        bindInput(sourceColumns);
        if (boundPositionAccumulator != null) {
            boundPositionAccumulator.add(inputPosition);
        }
        else {
            activePosition[0] = inputPosition;
            implementation.addRawInput(
                    state,
                    0,
                    Mask.sparse(activePosition, inputSize(sourceColumns, inputPosition)),
                    boundInput);
        }
        if (frame == Frame.RUNNING_ROWS) {
            Streams direct = implementation.copyResultPosition(
                    0,
                    0,
                    state,
                    output,
                    outputPosition,
                    outputSize,
                    allocator,
                    allocationContext);
            if (direct != null) {
                return direct;
            }
            result = implementation.result(0, state, result, allocator, allocationContext);
            output = copyResultPosition(allocator, allocationContext, result, output, outputPosition, outputSize);
        }
        return output;
    }

    private void bindInput(Streams[] sourceColumns)
    {
        if (sourceColumns == boundSourceColumns) {
            return;
        }
        boundSourceColumns = sourceColumns;
        boundInput = (argument, stream) -> {
            if (argument < 0 || argument >= inputColumns.length) {
                throw new IndexOutOfBoundsException("aggregate input: " + argument);
            }
            return sourceColumns[inputColumns[argument]].getOrNull(stream);
        };
        boundPositionAccumulator = implementation.bindRawInputPosition(state, 0, boundInput);
    }

    @Override
    public Streams finishPartition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            int partitionStart,
            int partitionEnd)
    {
        if (frame == Frame.RUNNING_ROWS || partitionStart == partitionEnd) {
            return output;
        }
        result = implementation.result(0, state, result, allocator, allocationContext);
        for (int outputPosition = partitionStart; outputPosition < partitionEnd; outputPosition++) {
            output = copyResultPosition(allocator, allocationContext, result, output, outputPosition, partitionEnd);
        }
        return output;
    }

    private void requireInitialized()
    {
        if (state == null) {
            throw new IllegalStateException("aggregate window output is not initialized");
        }
    }

    private static int inputSize(Streams[] sourceColumns, int inputPosition)
    {
        for (Streams column : sourceColumns) {
            for (Stream stream : column.streams()) {
                return column.get(stream).length();
            }
        }
        return inputPosition + 1;
    }

    private static Streams copyResultPosition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams source,
            Streams target,
            int outputPosition,
            int outputSize)
    {
        Streams.Builder output = Streams.builder();
        for (Stream stream : source.streams()) {
            Vector existing = target.getOrNull(stream);
            output.put(
                    stream,
                    source.get(stream).copySinglePositionInto(
                            allocator,
                            allocationContext,
                            existing,
                            0,
                            outputPosition,
                            outputSize));
        }
        return output.build();
    }

    public enum Frame
    {
        RUNNING_ROWS,
        FULL_PARTITION
    }
}
