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
package org.weakref.nitro.function.scalar;

import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.NullPropagatingScalarInvocationProvider;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** Exact non-null scalar target for one bound signature whose null propagation is framework-managed. */
public record NullPropagatingScalarMethodTarget(BoundSignature signature, MethodHandle methodHandle)
        implements NullPropagatingScalarInvocationProvider
{
    public NullPropagatingScalarMethodTarget
    {
        requireNonNull(signature, "signature is null");
        requireNonNull(methodHandle, "methodHandle is null");
    }

    @Override
    public Optional<MethodHandle> target(BoundSignature requestedSignature)
    {
        return signature.equals(requestedSignature) ? Optional.of(methodHandle) : Optional.empty();
    }
}
