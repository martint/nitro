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

import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedStateUpdate;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** A registry aggregate with provider-declared metadata for the generated grouped loop. */
public final class GeneratedRegisteredAggregationUnit
        extends RegisteredAggregationUnit
        implements GeneratedGroupedAggregationUnit
{
    private final GroupedAggregationUpdate update;
    private final boolean intermediateInput;

    public GeneratedRegisteredAggregationUnit(
            AggregationImplementation implementation,
            InputMode inputMode,
            OutputMode outputMode,
            int[] inputColumns,
            int filterInputColumn,
            GroupedAggregationUpdate update)
    {
        super(implementation, inputMode, outputMode, inputColumns, filterInputColumn);
        this.update = requireNonNull(update, "update is null");
        this.intermediateInput = inputMode == InputMode.INTERMEDIATE;
    }

    @Override
    public List<GroupedAggregationUpdate> generatedGroupedUpdates()
    {
        return List.of(update);
    }

    @Override
    public void bindGeneratedGroupedState(Object state, GroupedStateUpdate[] targets, int offset)
    {
        targets[offset] = (GroupedStateUpdate) state;
    }

    @Override
    public boolean mergesIntermediateInput()
    {
        return intermediateInput;
    }
}
