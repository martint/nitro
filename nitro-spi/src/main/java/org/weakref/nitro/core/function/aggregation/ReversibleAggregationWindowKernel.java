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
package org.weakref.nitro.core.function.aggregation;

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Streams;

/** Provider-owned tight loop for a forward batch of physically bound reversible window frames. */
public interface ReversibleAggregationWindowKernel
{
    default void reset() {}

    /** Exact primitive result shape, or {@code null} when this kernel only materializes vectors. */
    default PrimitiveRangeContribution primitiveResultContribution()
    {
        return null;
    }

    /** Allocates or reuses the complete destination before {@link #process} enters its position loop. */
    Streams prepareOutput(
            Streams existing,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext);

    /**
     * Processes contiguous output positions. Empty frames have a negative start. The provider owns state, null,
     * inverse, and result semantics; the engine owns only physical frame binding and destination placement.
     */
    Work process(
            AggregationWindowPartition partition,
            AggregationWindowFrameBounds bounds,
            int rangeStart,
            int positionCount,
            Streams output,
            int outputStart);

    /** Processes the same range while emitting results directly to a compatible provider consumer. */
    default Work process(
            AggregationWindowPartition partition,
            AggregationWindowFrameBounds bounds,
            int rangeStart,
            int positionCount,
            PrimitiveRangeConsumer consumer)
    {
        throw new UnsupportedOperationException("primitive range output is not supported");
    }

    record Work(long positions, long additions, long removals, long results) {}
}
