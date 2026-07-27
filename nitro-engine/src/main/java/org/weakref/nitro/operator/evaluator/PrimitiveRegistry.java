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
import org.weakref.nitro.core.function.ResolvedCall;
import org.weakref.nitro.core.function.projection.ProjectionCodeProvider;
import org.weakref.nitro.function.scalar.PrimitiveFunction;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.operator.evaluator.ir.Call;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class PrimitiveRegistry
{
    private final Map<String, PrimitiveFunction> functions = new LinkedHashMap<>();
    private final Map<String, List<FunctionCapability>> capabilities = new LinkedHashMap<>();

    public void register(String name, PrimitiveFunction function)
    {
        register(name, function, List.of());
    }

    public void register(String name, PrimitiveFunction function, FunctionCapability... capabilities)
    {
        register(name, function, List.of(capabilities));
    }

    private void register(String name, PrimitiveFunction function, List<FunctionCapability> functionCapabilities)
    {
        requireNonNull(name, "name is null");
        requireNonNull(function, "function is null");
        checkArgument(functions.putIfAbsent(name, function) == null, "Primitive function already registered: %s", name);
        capabilities.put(name, List.copyOf(functionCapabilities));
    }

    public void register(ScalarDescriptor descriptor)
    {
        register(descriptor.name(), descriptor.implementation(), descriptor.capabilities());
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
        register(call.identity().value(), binding.function(), binding.capabilities());
    }

    public PrimitiveFunction get(String name)
    {
        PrimitiveFunction function = functions.get(name);
        checkArgument(function != null, "Unknown primitive function: %s", name);
        return function;
    }

    public <T extends FunctionCapability> Optional<T> capability(Call call, Class<T> capabilityType)
    {
        return Optional.ofNullable(capabilityOrNull(call, capabilityType));
    }

    public <T extends FunctionCapability> T capabilityOrNull(Call call, Class<T> capabilityType)
    {
        requireNonNull(capabilityType, "capabilityType is null");
        if (call.resolvedCall() != null) {
            return call.resolvedCall().capability(capabilityType).orElse(null);
        }
        List<FunctionCapability> callCapabilities = capabilities.get(call.name());
        if (callCapabilities == null) {
            return null;
        }
        for (FunctionCapability capability : callCapabilities) {
            if (capabilityType.isInstance(capability)) {
                return capabilityType.cast(capability);
            }
        }
        return null;
    }

    public Optional<ProjectionCodeProvider> projectionCodeProvider(String name)
    {
        Optional<ProjectionCodeProvider> capability = capability(capabilities.get(name), ProjectionCodeProvider.class);
        if (capability.isPresent()) {
            return capability;
        }
        PrimitiveFunction function = functions.get(name);
        return function instanceof ProjectionCodeProvider provider ? Optional.of(provider) : Optional.empty();
    }

    public Optional<ProjectionCodeProvider> projectionCodeProvider(ResolvedCall call)
    {
        requireNonNull(call, "call is null");
        Optional<ProjectionCodeProvider> capability = call.capability(ProjectionCodeProvider.class);
        if (capability.isPresent()) {
            return capability;
        }
        if (!(call.invocation() instanceof PrimitiveInvocationBinding binding)) {
            return Optional.empty();
        }
        return binding.function() instanceof ProjectionCodeProvider provider ? Optional.of(provider) : Optional.empty();
    }

    private static <T extends FunctionCapability> Optional<T> capability(
            List<FunctionCapability> capabilities,
            Class<T> capabilityType)
    {
        if (capabilities == null) {
            return Optional.empty();
        }
        for (FunctionCapability capability : capabilities) {
            if (capabilityType.isInstance(capability)) {
                return Optional.of(capabilityType.cast(capability));
            }
        }
        return Optional.empty();
    }
}
