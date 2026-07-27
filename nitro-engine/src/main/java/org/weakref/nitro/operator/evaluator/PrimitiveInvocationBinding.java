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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.core.function.FunctionCapability;
import org.weakref.nitro.core.function.InvocationBinding;
import org.weakref.nitro.function.scalar.PrimitiveFunction;

import java.util.List;

import static java.util.Objects.requireNonNull;

/// Binding of a dynamically resolved call to Nitro's current vector primitive protocol.
public record PrimitiveInvocationBinding(PrimitiveFunction function, List<FunctionCapability> capabilities)
        implements InvocationBinding
{
    public PrimitiveInvocationBinding(PrimitiveFunction function)
    {
        this(function, List.of());
    }

    public PrimitiveInvocationBinding
    {
        function = requireNonNull(function, "function is null");
        capabilities = List.copyOf(capabilities);
    }
}
