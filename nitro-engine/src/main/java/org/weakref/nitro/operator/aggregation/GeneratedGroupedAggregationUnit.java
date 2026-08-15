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

import java.util.List;

/**
 * Engine-lowered physical unit whose grouped updates can be emitted into one generated loop.
 *
 * <p>The unit owns its opaque state layout and explicitly binds each declared update to a state
 * target. The operator knows neither the state representation nor the number of updates per unit.
 */
public interface GeneratedGroupedAggregationUnit
        extends PhysicalAggregationUnit
{
    List<GroupedAggregationUpdate> generatedGroupedUpdates();

    void bindGeneratedGroupedState(Object state, GroupedStateUpdate[] targets, int offset);

    /** True when the generated update merges an intermediate aggregate state rather than raw rows. */
    default boolean mergesIntermediateInput()
    {
        return false;
    }
}
