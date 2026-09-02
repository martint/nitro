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

import java.lang.invoke.MethodHandle;
import java.util.Optional;

/**
 * A non-null scalar target for a function whose result is null when any argument is null.
 *
 * <p>The function's ordinary batch implementation remains authoritative. This optional capability lets the
 * framework implement its null propagation and invoke the returned exact target only for non-null arguments. A
 * provider must expose this capability only when all of the following hold for the bound signature:
 *
 * <ul>
 *     <li>any null argument produces a null result without observable function invocation;</li>
 *     <li>all non-null arguments produce a non-null result; and</li>
 *     <li>the target has the exact primitive carrier signature described by the binding.</li>
 * </ul>
 *
 * <p>The signature is an input because parametric and variadic functions may need a different target for each
 * resolved call. Returning empty declines the capability for that signature.
 */
public interface NullPropagatingScalarInvocationProvider
        extends FunctionCapability
{
    Optional<MethodHandle> target(BoundSignature signature);
}
