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
import org.weakref.nitro.operator.AdaptiveLongGroupingPolicy;
import org.weakref.nitro.operator.DistinctKeySetPolicy;
import org.weakref.nitro.operator.FlatKeyTablePolicy;
import org.weakref.nitro.operator.OperatorCodeGenerationResources;

import static java.util.Objects.requireNonNull;

/**
 * Typed services supplied to a dynamically registered accumulator for one operator instance.
 */
public record AggregationExecutionContext(
        Allocator allocator,
        Allocator.Context allocationContext,
        OperatorCodeGenerationResources codeGeneration,
        DistinctKeySetPolicy distinctKeySetPolicy,
        AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
        FlatKeyTablePolicy flatKeyTablePolicy)
{
    public AggregationExecutionContext
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(allocationContext, "allocationContext is null");
        requireNonNull(codeGeneration, "codeGeneration is null");
        requireNonNull(distinctKeySetPolicy, "distinctKeySetPolicy is null");
        requireNonNull(adaptiveLongGroupingPolicy, "adaptiveLongGroupingPolicy is null");
        requireNonNull(flatKeyTablePolicy, "flatKeyTablePolicy is null");
    }
}
