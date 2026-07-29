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
import org.weakref.nitro.core.type.TypeBinding;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Optional state protocol supplied by a dynamically loaded aggregate provider.
 */
public interface AggregationImplementationProvider
        extends FunctionCapability
{
    Optional<Binding> bind(List<AggregationArgument> arguments);

    /**
     * One function's state protocol and its portable intermediate-state type.
     *
     * <p>The current transport has one intermediate column. Providers needing multiple state
     * fields must expose a registry-owned structured type rather than leak a host row object.
     */
    record Binding(TypeBinding intermediateType, AggregationImplementation implementation)
    {
        public Binding
        {
            intermediateType = requireNonNull(intermediateType, "intermediateType is null");
            implementation = requireNonNull(implementation, "implementation is null");
        }
    }
}
