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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// Exact, already-specialized scalar semantics supplied by a function registry.
///
/// The handle type is the calling convention: its parameters and return type are JVM stack
/// carriers. Logical types and null/failure behavior remain in the resolved call metadata.
public final class ScalarMethodTarget
{
    private final MethodHandle handle;
    private final DirectInvocation directInvocation;

    public ScalarMethodTarget(MethodHandle handle)
    {
        this(handle, null);
    }

    private ScalarMethodTarget(MethodHandle handle, DirectInvocation directInvocation)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.directInvocation = directInvocation;
    }

    /**
     * Supplies a direct scalar target that generated code may link symbolically.
     *
     * <p>The lookup keeps generation in the provider's classloader and access domain. The exact method handle
     * remains the classloader-neutral fallback and the source of the bound JVM calling convention.
     */
    public static ScalarMethodTarget direct(MethodHandles.Lookup lookup, MethodHandle handle)
    {
        requireNonNull(lookup, "lookup is null");
        requireNonNull(handle, "handle is null");
        MethodHandleInfo info = lookup.revealDirect(handle);
        checkArgument(info.getReferenceKind() == MethodHandleInfo.REF_invokeStatic,
                "Direct scalar generation currently requires a static target");
        return new ScalarMethodTarget(handle, new DirectInvocation(lookup, info.getDeclaringClass(), info.getName()));
    }

    public MethodHandle handle()
    {
        return handle;
    }

    DirectInvocation directInvocation()
    {
        return directInvocation;
    }

    record DirectInvocation(MethodHandles.Lookup lookup, Class<?> declaringClass, String name) {}
}
