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
package org.weakref.nitro.core.function;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// Complete plan-time function binding consumed as data by execution.
public record ResolvedCall(
        FunctionIdentity identity,
        BoundSignature signature,
        FunctionSemantics semantics,
        List<ResolvedCall> dependencies,
        InvocationBinding invocation)
{
    public ResolvedCall
    {
        identity = requireNonNull(identity, "identity is null");
        signature = requireNonNull(signature, "signature is null");
        semantics = requireNonNull(semantics, "semantics is null");
        dependencies = List.copyOf(requireNonNull(dependencies, "dependencies is null"));
        invocation = requireNonNull(invocation, "invocation is null");
        if (signature.argumentTypes().size() != semantics.argumentNullConventions().size()) {
            throw new IllegalArgumentException("signature and null conventions have different arities");
        }
    }

    public <T extends FunctionCapability> Optional<T> capability(Class<T> capabilityType)
    {
        requireNonNull(capabilityType, "capabilityType is null");
        return invocation.capabilities().stream()
                .filter(capabilityType::isInstance)
                .map(capabilityType::cast)
                .findFirst();
    }
}
