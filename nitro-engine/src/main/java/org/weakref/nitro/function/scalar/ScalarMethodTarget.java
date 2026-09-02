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

import org.weakref.nitro.core.function.ScalarInvocationProvider;

import java.lang.invoke.MethodHandle;

import static java.util.Objects.requireNonNull;

/// Exact, already-specialized scalar semantics supplied by a function registry.
///
/// The handle type is the calling convention: its parameters and return type are JVM stack
/// carriers. Logical types and null/failure behavior remain in the resolved call metadata.
public final class ScalarMethodTarget
        implements ScalarInvocationProvider
{
    private final MethodHandle handle;

    public ScalarMethodTarget(MethodHandle handle)
    {
        this.handle = requireNonNull(handle, "handle is null");
    }

    @Override
    public MethodHandle target()
    {
        return handle;
    }
}
