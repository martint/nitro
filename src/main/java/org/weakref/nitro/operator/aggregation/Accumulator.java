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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.Streams;

/**
 * Stateful aggregation contract used by global and grouped aggregation operators.
 * <p>
 * Accumulators manage their own state layout in {@link Streams}, grow that state as the number of
 * groups increases, and eventually materialize result streams from the accumulated state.
 */
public interface Accumulator
{
    /**
     * Allocates initial state capable of holding at least {@code size} groups.
     */
    Streams allocate(Allocator allocator, Allocator.Context allocationContext, int size);

    /**
     * Ensures the supplied state can hold at least {@code size} groups.
     */
    Streams grow(Allocator allocator, Allocator.Context allocationContext, Streams state, int size);

    /**
     * Initializes the state slots in {@code [offset, offset + length)}.
     */
    void initialize(Streams state, int offset, int length);

    /**
     * Accumulates rows selected by {@code mask} into a single known group id.
     */
    void accumulate(Streams state, int group, Mask mask, StreamAccessor streams);

    /**
     * Accumulates rows selected by {@code mask} using per-row group ids from {@code groups}.
     */
    void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams);

    /**
     * Materializes result streams for the selected result mask.
     * <p>
     * Implementations may override this to avoid materializing values for groups outside
     * {@code mask}.
     */
    default Streams result(int maxGroup, Streams state, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        return result(maxGroup, state, output, allocator, allocationContext);
    }

    /**
     * Materializes result streams for groups {@code 0..maxGroup}.
     */
    Streams result(int maxGroup, Streams state, Streams output, Allocator allocator, Allocator.Context allocationContext);
}
