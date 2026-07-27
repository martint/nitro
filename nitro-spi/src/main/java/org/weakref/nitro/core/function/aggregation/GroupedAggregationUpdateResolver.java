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

import org.weakref.nitro.core.function.ResolvedCall;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Generic plan-time lowering from registry capability metadata to a bound physical update.
 */
public final class GroupedAggregationUpdateResolver
{
    public Optional<GroupedAggregationUpdate> resolve(ResolvedCall call, List<AggregationArgumentBinding> arguments)
    {
        requireNonNull(call, "call is null");
        List<AggregationArgumentBinding> bindings = List.copyOf(requireNonNull(arguments, "arguments is null"));
        if (call.signature().argumentTypes().size() != bindings.size()) {
            throw new IllegalArgumentException("call signature and argument bindings have different arities");
        }
        List<AggregationArgument> providerArguments = bindings.stream()
                .map(AggregationArgumentBinding::argument)
                .toList();
        return call.capability(GroupedAggregationUpdateProvider.class)
                .flatMap(provider -> provider.update(providerArguments))
                .map(template -> template.bind(bindings));
    }
}
