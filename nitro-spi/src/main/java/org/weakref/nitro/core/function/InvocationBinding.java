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

/// Classloader-neutral base for a registry-supplied invocation protocol.
///
/// The core plan never reflects on or names a provider implementation class. Optional behavior is
/// exposed through SPI capability interfaces, so physical lowering can query a resolved call
/// without downcasting to a concrete invocation protocol.
public interface InvocationBinding
{
    default List<FunctionCapability> capabilities()
    {
        return List.of();
    }
}
