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

import java.lang.reflect.Method;
import java.util.List;

import static java.util.Objects.requireNonNull;

public record ScalarDescriptor(
        String name,
        String returnType,
        List<String> argumentTypes,
        boolean deterministic,
        Method implementation)
{
    public ScalarDescriptor
    {
        requireNonNull(name, "name is null");
        requireNonNull(returnType, "returnType is null");
        argumentTypes = List.copyOf(argumentTypes);
        requireNonNull(implementation, "implementation is null");
    }
}
