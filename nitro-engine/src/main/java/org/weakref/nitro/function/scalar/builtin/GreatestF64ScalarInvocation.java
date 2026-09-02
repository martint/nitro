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
package org.weakref.nitro.function.scalar.builtin;

import org.weakref.nitro.core.function.BoundSignature;
import org.weakref.nitro.core.function.NullPropagatingScalarInvocationProvider;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class GreatestF64ScalarInvocation
        implements NullPropagatingScalarInvocationProvider
{
    private static final MethodHandle BINARY_MAXIMUM = binaryMaximum();

    @Override
    public Optional<MethodHandle> target(BoundSignature signature)
    {
        requireNonNull(signature, "signature is null");
        if (signature.resultType().carrierType() != double.class ||
                signature.argumentTypes().isEmpty() ||
                signature.argumentTypes().stream().anyMatch(type -> type.carrierType() != double.class)) {
            return Optional.empty();
        }

        if (signature.argumentTypes().size() == 1) {
            return Optional.of(MethodHandles.identity(double.class));
        }

        MethodHandle target = BINARY_MAXIMUM;
        for (int argument = 2; argument < signature.argumentTypes().size(); argument++) {
            target = MethodHandles.collectArguments(BINARY_MAXIMUM, 0, target);
        }
        return Optional.of(target);
    }

    private static double maximum(double left, double right)
    {
        return Math.max(left, right);
    }

    private static MethodHandle binaryMaximum()
    {
        try {
            return MethodHandles.lookup().findStatic(
                    GreatestF64ScalarInvocation.class,
                    "maximum",
                    MethodType.methodType(double.class, double.class, double.class));
        }
        catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }
}
