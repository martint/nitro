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

import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedStateUpdate;
import org.weakref.nitro.core.function.aggregation.LongStateUpdate;
import org.weakref.nitro.data.Streams;

import java.util.List;

/**
 * Marks an {@link Accumulator} whose state implements the long-update SPI and whose provider declares
 * its per-row contribution, so it can participate in the generated single-long-key grouped-aggregation kernel — one
 * inlined pass that probes the group table and accumulates with no group-id vector and no per-row
 * accumulator dispatch.
 */
public interface GeneratedGroupedAccumulator
        extends Accumulator, GeneratedGroupedAggregationUnit
{
    /**
     * Declares the provider-supplied physical update that a generated grouping loop executes.
     */
    GroupedAggregationUpdate generatedGroupedUpdate();

    @Override
    default List<GroupedAggregationUpdate> generatedGroupedUpdates()
    {
        return List.of(generatedGroupedUpdate());
    }

    @Override
    default void bindGeneratedGroupedState(Object state, GroupedStateUpdate[] targets, int offset)
    {
        targets[offset] = (LongStateUpdate) ((Streams) state).values();
    }
}
