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
 * Resolves a function capability without exposing physical input columns to its provider.
 */
public final class ResolvedAggregation
{
    private ResolvedAggregation() {}

    public static Optional<AggregationImplementationProvider.Binding> resolve(
            ResolvedCall call,
            List<AggregationArgument> arguments)
    {
        requireNonNull(call, "call is null");
        List<AggregationArgument> copiedArguments = List.copyOf(requireNonNull(arguments, "arguments is null"));
        if (call.signature().argumentTypes().size() != copiedArguments.size()) {
            throw new IllegalArgumentException("call signature and aggregate arguments have different arities");
        }
        return call.capability(AggregationImplementationProvider.class)
                .flatMap(provider -> requireNonNull(
                        provider.bind(copiedArguments),
                        "aggregation implementation provider returned null"));
    }
}
