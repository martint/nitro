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

/**
 * An exact scalar invocation target supplied by a function registry.
 *
 * <p>The resolved call remains authoritative for the logical signature and invocation semantics. This capability
 * exposes only executable scalar semantics, allowing an engine-owned adapter or expression compiler to link the
 * target without recognizing the function or its provider class.
 */
public interface ScalarInvocationProvider
        extends FunctionCapability
{
    MethodHandle target();
}
