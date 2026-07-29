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
    Object allocate(AggregationExecution execution, int groups);

    Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups);

    void initialize(Object state, int offset, int length);

    void addRawInput(Object state, int group, Mask mask, AggregationInput input);

    void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input);

    void addIntermediate(Object state, int group, Mask mask, AggregationInput input);

    void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input);

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
}
