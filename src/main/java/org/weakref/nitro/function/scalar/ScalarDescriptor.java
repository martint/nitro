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

import org.weakref.nitro.operator.evaluator.PrimitiveFunction;

import java.lang.reflect.Method;

import static java.util.Objects.requireNonNull;

public record ScalarDescriptor(
        String name,
        boolean deterministic,
        Method implementation,
        Class<? extends PrimitiveFunction> vectorizedAdapter)
{
    public ScalarDescriptor
    {
        requireNonNull(name, "name is null");
        requireNonNull(implementation, "implementation is null");
        requireNonNull(vectorizedAdapter, "vectorizedAdapter is null");
    }

    public int arity()
    {
        return implementation.getParameterCount();
    }
}
