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

import org.weakref.nitro.core.function.FunctionCapability;

import java.util.List;
import java.util.Optional;

/**
 * Optional grouped-update lowering supplied by a dynamically loaded aggregate provider.
 */
public interface GroupedAggregationUpdateProvider
        extends FunctionCapability
{
    /** Describes the update applied while consuming raw aggregate arguments. */
    Optional<GroupedAggregationUpdateTemplate> update(List<AggregationArgument> arguments);

    /**
     * Describes the update applied while merging the aggregate's intermediate value. The template's
     * argument coordinates refer to the intermediate input rather than the aggregate's raw arguments.
     */
    default Optional<GroupedAggregationUpdateTemplate> intermediateUpdate()
    {
        return Optional.empty();
    }
}
