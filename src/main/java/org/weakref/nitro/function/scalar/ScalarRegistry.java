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

import org.weakref.nitro.core.function.FunctionCapability;
import org.weakref.nitro.operator.evaluator.PrimitiveFunction;

import java.lang.reflect.InvocationTargetException;
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
        checkArgument(PrimitiveFunction.class.isAssignableFrom(functionClass), "Function class must implement PrimitiveFunction: %s", functionClass.getName());

        ScalarDescriptor descriptor = new ScalarDescriptor(
                scalarFunction.name(),
                scalarFunction.deterministic(),
                instantiate(functionClass.asSubclass(PrimitiveFunction.class)),
                java.util.Arrays.stream(scalarFunction.capabilities())
                        .map(ScalarRegistry::instantiateCapability)
                        .toList());

        checkArgument(descriptors.putIfAbsent(descriptor.name(), descriptor) == null, "Scalar function already registered: %s", descriptor.name());
        return descriptor;
    }

    public ScalarDescriptor get(String name)
    {
        ScalarDescriptor descriptor = descriptors.get(name);
        checkArgument(descriptor != null, "Unknown scalar function: %s", name);
        return descriptor;
    }

    private static PrimitiveFunction instantiate(Class<? extends PrimitiveFunction> functionClass)
    {
        return (PrimitiveFunction) instantiateComponent(functionClass);
    }

    private static FunctionCapability instantiateCapability(Class<? extends FunctionCapability> capabilityClass)
    {
        return (FunctionCapability) instantiateComponent(capabilityClass);
    }

    private static Object instantiateComponent(Class<?> implementationClass)
    {
        try {
            var constructor = implementationClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException exception) {
            throw new IllegalArgumentException("Unable to instantiate scalar function component: " + implementationClass.getName(), exception);
        }
    }
}
