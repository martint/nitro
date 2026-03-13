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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.function.scalar.ScalarDescriptor;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class PrimitiveRegistry
{
    private final Map<String, PrimitiveFunction> functions = new LinkedHashMap<>();

    public void register(String name, PrimitiveFunction function)
    {
        requireNonNull(name, "name is null");
        requireNonNull(function, "function is null");
        checkArgument(functions.putIfAbsent(name, function) == null, "Primitive function already registered: %s", name);
    }

    public void register(ScalarDescriptor descriptor)
    {
        register(descriptor.name(), bind(descriptor));
    }

    public PrimitiveFunction get(String name)
    {
        PrimitiveFunction function = functions.get(name);
        checkArgument(function != null, "Unknown primitive function: %s", name);
        return function;
    }

    private static PrimitiveFunction bind(ScalarDescriptor descriptor)
    {
        if (descriptor.vectorizedAdapter() == PrimitiveFunction.class) {
            return new InterpretedScalarFunction(descriptor);
        }

        try {
            return descriptor.vectorizedAdapter()
                    .asSubclass(PrimitiveFunction.class)
                    .getConstructor(ScalarDescriptor.class)
                    .newInstance(descriptor);
        }
        catch (NoSuchMethodException ignored) {
            try {
                return descriptor.vectorizedAdapter()
                        .asSubclass(PrimitiveFunction.class)
                        .getConstructor()
                        .newInstance();
            }
            catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException exception) {
                throw new IllegalArgumentException("Unable to instantiate vectorized adapter for " + descriptor.name(), exception);
            }
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException exception) {
            throw new IllegalArgumentException("Unable to instantiate vectorized adapter for " + descriptor.name(), exception);
        }
    }
}
