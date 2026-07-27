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

import java.lang.reflect.InvocationTargetException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// Compatibility loader for annotation-declared, no-argument scalar components.
///
/// Dynamic providers may supply an explicitly constructed [PrimitiveFunction] to retain annotation-declared
/// metadata, or construct [ScalarDescriptor] instances directly when they own all metadata.
public final class AnnotatedScalarLoader
{
    public ScalarDescriptor load(Class<?> functionClass)
    {
        requireNonNull(functionClass, "functionClass is null");

        ScalarFunction scalarFunction = functionClass.getAnnotation(ScalarFunction.class);
        checkArgument(scalarFunction != null, "Function class is missing @ScalarFunction: %s", functionClass.getName());
        checkArgument(PrimitiveFunction.class.isAssignableFrom(functionClass), "Function class must implement PrimitiveFunction: %s", functionClass.getName());

        return load(scalarFunction, instantiate(functionClass.asSubclass(PrimitiveFunction.class)));
    }

    public ScalarDescriptor load(PrimitiveFunction implementation)
    {
        requireNonNull(implementation, "implementation is null");
        Class<?> functionClass = implementation.getClass();
        ScalarFunction scalarFunction = functionClass.getAnnotation(ScalarFunction.class);
        checkArgument(scalarFunction != null, "Function class is missing @ScalarFunction: %s", functionClass.getName());
        return load(scalarFunction, implementation);
    }

    private static ScalarDescriptor load(ScalarFunction scalarFunction, PrimitiveFunction implementation)
    {
        return new ScalarDescriptor(
                scalarFunction.name(),
                scalarFunction.deterministic(),
                implementation,
                java.util.Arrays.stream(scalarFunction.capabilities())
                        .map(AnnotatedScalarLoader::instantiateCapability)
                        .toList());
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
