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

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Streams;

import static java.util.Objects.requireNonNull;

/** Selects the immediately preceding or following physical value within each partition. */
public final class AdjacentValueWindowFunction
        implements RunningWindowFunction
{
    private final TypeVectorFactory vectorFactory;
    private final int inputColumn;
    private final Direction direction;

    private Streams previous;
    private int previousPosition;
    private boolean outputValuesInitialized;

    public AdjacentValueWindowFunction(TypeBinding outputType, int inputColumn, Direction direction)
    {
        requireNonNull(outputType, "outputType is null");
        this.vectorFactory = outputType.vectorFactory()
                .orElseThrow(() -> new IllegalArgumentException("outputType has no vector factory"));
        if (inputColumn < 0) {
            throw new IllegalArgumentException("inputColumn is negative");
        }
        this.inputColumn = inputColumn;
        this.direction = requireNonNull(direction, "direction is null");
    }

    @Override
    public Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size)
    {
        outputValuesInitialized = false;
        return WindowValueCopySupport.emptyOutput(vectorFactory, allocator, allocationContext, size, true);
    }

    @Override
    public void reset()
    {
        previous = null;
        previousPosition = -1;
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
        Streams current = sourceColumns[inputColumn];
        if (direction == Direction.PRECEDING && previous != null) {
            output = copy(allocator, allocationContext, previous, output, previousPosition, outputPosition, outputSize);
        }
        else if (direction == Direction.FOLLOWING && previous != null) {
            output = copy(allocator, allocationContext, current, output, inputPosition, outputPosition - 1, outputSize);
        }
        previous = current;
        previousPosition = inputPosition;
        return output;
    }

    private Streams copy(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams source,
            Streams output,
            int sourcePosition,
            int outputPosition,
            int outputSize)
    {
        Streams result = WindowValueCopySupport.copyRange(
                allocator,
                allocationContext,
                source,
                output,
                outputValuesInitialized,
                sourcePosition,
                outputPosition,
                outputPosition + 1,
                outputSize);
        outputValuesInitialized = true;
        return result;
    }

    public enum Direction
    {
        PRECEDING,
        FOLLOWING
    }
}
