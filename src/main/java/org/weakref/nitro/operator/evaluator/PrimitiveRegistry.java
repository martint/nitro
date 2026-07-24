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

import org.weakref.nitro.core.function.ResolvedCall;
import org.weakref.nitro.function.scalar.ScalarDescriptor;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class PrimitiveRegistry
{
    private final Map<String, PrimitiveFunction> functions = new LinkedHashMap<>();

    public void register(String name, PrimitiveFunction function)
    {
        requireNonNull(name, "name is null");
        requireNonNull(function, "function is null");
        checkArgument(functions.putIfAbsent(name, function) == null, "Primitive function already registered: %s", name);
    }

    public void register(ScalarDescriptor descriptor)
    {
        register(descriptor.name(), descriptor.implementation());
    }

    public void register(ResolvedCall call)
    {
        requireNonNull(call, "call is null");
        if (!(call.invocation() instanceof PrimitiveInvocationBinding binding)) {
            throw new IllegalArgumentException("Resolved call does not provide a primitive invocation: " + call.identity());
        }
        if (call.semantics().deterministic() != binding.function().deterministic()) {
            throw new IllegalArgumentException("Resolved call determinism does not match its primitive invocation: " + call.identity());
        }
        register(call.identity().value(), binding.function());
    }

    public PrimitiveFunction get(String name)
    {
        PrimitiveFunction function = functions.get(name);
        checkArgument(function != null, "Unknown primitive function: %s", name);
        return function;
    }
}
