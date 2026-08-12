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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

/**
 * Classloader-neutral state protocol supplied by a dynamically resolved aggregate.
 *
 * <p>The implementation owns the opaque state object. The engine selects raw or intermediate
 * input and intermediate or final output from the physical plan; it does not infer a function or
 * rewrite neighboring aggregates.
 */
public interface AggregationImplementation
{
    /**
     * Selects the logical state capacity for a required group count.
     *
     * <p>The default preserves the engine's amortized-growth capacity. Implementations backed by
     * incrementally allocated storage may return {@code requiredGroups} to avoid unused geometric
     * headroom and whole-state copy peaks.
     */
    default int stateCapacity(int requiredGroups, int defaultCapacity)
    {
        return defaultCapacity;
    }

    Object allocate(AggregationExecution execution, int groups);

    Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups);

    void initialize(Object state, int offset, int length);

    void addRawInput(Object state, int group, Mask mask, AggregationInput input);

    void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input);

    /**
     * Optionally binds a direct single-position update to one physical input batch.
     *
     * <p>Window execution uses this capability for running frames. Returning {@code null} retains the ordinary
     * mask-based update. The implementation, rather than the engine, owns physical type and function semantics.
     */
    default AggregationPositionAccumulator bindRawInputPosition(Object state, int group, AggregationInput input)
    {
        return null;
    }

    void addIntermediate(Object state, int group, Mask mask, AggregationInput input);

    void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input);

    /**
     * Whether this implementation can lower selected raw rows directly to independent intermediate states.
     */
    default boolean supportsInitialRawIntermediate()
    {
        return false;
    }

    /**
     * Lowers selected raw rows directly to independent intermediate states.
     *
     * <p>Adaptive partial aggregation uses this when every input row is deliberately passed as its own group. The
     * function implementation owns the physical state representation; the execution engine does not recognize
     * individual aggregate functions or types.
     */
    default Streams initialRawIntermediate(
            Mask mask,
            AggregationInput input,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        throw new UnsupportedOperationException("direct initial intermediate state is not supported");
    }

    Streams intermediate(
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext);

    default Streams intermediate(
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return intermediate(maxGroup, state, existing, allocator, allocationContext);
    }

    Streams result(
            int maxGroup,
            Object state,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext);

    default Streams result(
            int maxGroup,
            Object state,
            Mask mask,
            Streams existing,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return result(maxGroup, state, existing, allocator, allocationContext);
    }

    /**
     * Copies one intermediate result position without materializing every group.
     *
     * <p>Returning {@code null} requests the engine's full-result fallback.
     */
    default Streams copyIntermediatePosition(
            int group,
            int maxGroup,
            Object state,
            Streams existing,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return null;
    }

    /**
     * Copies one final result position without materializing every group.
     *
     * <p>Returning {@code null} requests the engine's full-result fallback.
     */
    default Streams copyResultPosition(
            int group,
            int maxGroup,
            Object state,
            Streams existing,
            int outputPosition,
            int size,
            Allocator allocator,
            Allocator.Context allocationContext)
    {
        return null;
    }

    default Streams copyResultRange(
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
        return null;
    }

    default Streams copyIntermediateRange(
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
        return null;
    }
}
