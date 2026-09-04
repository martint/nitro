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

import org.weakref.nitro.core.execution.ExecutionDiagnostics;
import org.weakref.nitro.core.function.aggregation.AggregationWindowFrameBounds;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeConsumer;
import org.weakref.nitro.core.function.aggregation.PrimitiveRangeContribution;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Streams;

/**
 * Pluggable contract that {@link WindowOperator} drives once per input row per partition. Window
 * functions accumulate per-row state (or whole-partition state emitted at finish) into the output
 * {@link Streams} passed to them; the operator has no knowledge of which concrete function is
 * running.
 */
public interface RunningWindowFunction
{
    Streams emptyOutput(Allocator allocator, Allocator.Context allocationContext, int size);

    void reset();

    Streams append(Allocator allocator, Allocator.Context allocationContext, Streams output, Streams[] sourceColumns, int inputPosition, int outputPosition, int outputSize);

    default Streams finishPartition(Allocator allocator, Allocator.Context allocationContext, Streams output, int partitionStart, int partitionEnd, int outputSize)
    {
        return output;
    }

    default Streams finishPartition(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            WindowPositionIndex partition,
            int partitionStart,
            int outputSize)
    {
        return finishPartition(
                allocator,
                allocationContext,
                output,
                partitionStart,
                partitionStart + partition.size(),
                outputSize);
    }

    default boolean supportsForwardBatchRangeMaterialization()
    {
        return false;
    }

    default PrimitiveRangeContribution primitiveRangeContribution()
    {
        return null;
    }

    default void emitForwardPrimitiveRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            WindowPositionIndex partition,
            int rangeStart,
            int rangeEnd,
            AggregationWindowFrameBounds bounds,
            PrimitiveRangeConsumer consumer)
    {
        throw new UnsupportedOperationException("primitive range output is not supported");
    }

    /**
     * Materializes a forward range of one complete partition into an independently owned batch result.
     * Calls for a partition begin at {@code rangeStart == 0} and advance without gaps. A function may
     * retain state between calls, but must reset that state when a new partition starts.
     */
    default Streams materializeForwardBatchRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            WindowPositionIndex partition,
            int rangeStart,
            int rangeEnd,
            int destinationStart,
            int destinationSize)
    {
        throw new UnsupportedOperationException("forward batch-range materialization is not supported");
    }

    default Object frameTraversalIdentity()
    {
        return this;
    }

    default WindowFrame.Cursor bindForwardBatchBounds(WindowPositionIndex partition)
    {
        throw new UnsupportedOperationException("forward batch bounds are not supported");
    }

    default Streams materializeForwardBatchRange(
            Allocator allocator,
            Allocator.Context allocationContext,
            Streams output,
            WindowPositionIndex partition,
            int rangeStart,
            int rangeEnd,
            int destinationStart,
            int destinationSize,
            AggregationWindowFrameBounds bounds)
    {
        return materializeForwardBatchRange(
                allocator,
                allocationContext,
                output,
                partition,
                rangeStart,
                rangeEnd,
                destinationStart,
                destinationSize);
    }

    default void reportDiagnostics(ExecutionDiagnostics diagnostics) {}
}
