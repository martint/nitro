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
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ScalarRegistry
{
    private final Map<String, ScalarDescriptor> descriptors = new LinkedHashMap<>();

    public ScalarDescriptor register(Class<?> functionClass)
    {
        requireNonNull(functionClass, "functionClass is null");

        ScalarFunction scalarFunction = functionClass.getAnnotation(ScalarFunction.class);
        checkArgument(scalarFunction != null, "Function class is missing @ScalarFunction: %s", functionClass.getName());

        Method implementation = findImplementation(functionClass);
        checkArgument(Modifier.isStatic(implementation.getModifiers()), "Scalar implementation must be static: %s", implementation);

        ScalarDescriptor descriptor = new ScalarDescriptor(
                scalarFunction.name(),
                scalarFunction.deterministic(),
                implementation,
                scalarFunction.vectorizedAdapter());

        checkArgument(descriptors.putIfAbsent(descriptor.name(), descriptor) == null, "Scalar function already registered: %s", descriptor.name());
        return descriptor;
    }

    public ScalarDescriptor get(String name)
    {
        ScalarDescriptor descriptor = descriptors.get(name);
        checkArgument(descriptor != null, "Unknown scalar function: %s", name);
        return descriptor;
    }

    private static Method findImplementation(Class<?> functionClass)
    {
        Method implementation = null;
        for (Method method : functionClass.getDeclaredMethods()) {
            if (!method.isAnnotationPresent(ScalarImplementation.class)) {
                continue;
            }
            checkArgument(implementation == null, "Multiple @ScalarImplementation methods found in %s", functionClass.getName());
            implementation = method;
        }
        checkArgument(implementation != null, "No @ScalarImplementation method found in %s", functionClass.getName());
        return implementation;
    }
}
