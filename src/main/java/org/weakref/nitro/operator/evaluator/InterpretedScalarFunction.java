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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.function.scalar.ScalarDescriptor;
import org.weakref.nitro.operator.Streams;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class InterpretedScalarFunction
        implements PrimitiveFunction
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("InterpretedScalarFunction");

    private final ScalarDescriptor descriptor;
    private final MethodHandle methodHandle;
    private final ScalarKind kind;

    public InterpretedScalarFunction(ScalarDescriptor descriptor)
    {
        this.descriptor = descriptor;
        this.kind = ScalarKind.forMethod(descriptor.implementation());
        try {
            this.methodHandle = MethodHandles.lookup().unreflect(descriptor.implementation());
        }
        catch (IllegalAccessException exception) {
            throw new IllegalArgumentException("Unable to bind scalar implementation: " + descriptor.name(), exception);
        }
    }

    @Override
    public Streams apply(List<Streams> inputs, Mask mask, Streams output, PrimitiveExecutionContext context)
    {
        checkArgument(inputs.size() == descriptor.arity(), "Unexpected argument count for %s", descriptor.name());

        return switch (kind) {
            case I64_BINARY -> Streams.ofValues(applyI64Binary(inputs, mask, output, context));
            case I64_COMPARISON -> Streams.of(Stream.VALUES, applyI64Comparison(inputs, mask, output, context));
            case BOOLEAN_BINARY -> Streams.of(Stream.VALUES, applyBooleanBinary(inputs, mask, output, context));
        };
    }

    private Vector applyI64Binary(List<Streams> inputs, Mask mask, Streams output, PrimitiveExecutionContext context)
    {
        return I64BinaryVectorSupport.applyLongBinary(
                descriptor.name(),
                inputs.get(0).values(),
                inputs.get(1).values(),
                mask,
                output != null && output.has(Stream.VALUES) ? output.values() : null,
                context,
                ALLOCATION_CONTEXT,
                (left, right) -> (long) methodHandle.invokeWithArguments(left, right));
    }

    private BooleanVector applyBooleanBinary(List<Streams> inputs, Mask mask, Streams output, PrimitiveExecutionContext context)
    {
        BooleanVector left = (BooleanVector) inputs.get(0).values();
        BooleanVector right = (BooleanVector) inputs.get(1).values();
        BooleanVector result = output != null && output.has(Stream.VALUES) ? (BooleanVector) output.values() : null;
        result = (BooleanVector) context.allocator().allocateOrGrow(ALLOCATION_CONTEXT, result, requiredLength(mask, left.length()), BooleanVector::new);

        try {
            for (int position : mask) {
                result.values()[position] = (boolean) methodHandle.invokeWithArguments(left.values()[position], right.values()[position]);
            }
        }
        catch (Throwable throwable) {
            throw new RuntimeException("Error invoking scalar function " + descriptor.name(), throwable);
        }
        return result;
    }

    private Vector applyI64Comparison(List<Streams> inputs, Mask mask, Streams output, PrimitiveExecutionContext context)
    {
        return I64BinaryVectorSupport.applyLongComparison(
                descriptor.name(),
                inputs.get(0).values(),
                inputs.get(1).values(),
                mask,
                output != null && output.has(Stream.VALUES) ? output.values() : null,
                context,
                ALLOCATION_CONTEXT,
                (left, right) -> (boolean) methodHandle.invokeWithArguments(left, right));
    }

    private static int requiredLength(Mask mask, int defaultLength)
    {
        if (mask.none()) {
            return defaultLength;
        }
        return Math.max(defaultLength, mask.maxPosition() + 1);
    }

    private enum ScalarKind
    {
        I64_BINARY,
        I64_COMPARISON,
        BOOLEAN_BINARY;

        private static ScalarKind forMethod(Method method)
        {
            Class<?>[] parameterTypes = method.getParameterTypes();
            if (method.getReturnType() == long.class && parameterTypes.length == 2 && parameterTypes[0] == long.class && parameterTypes[1] == long.class) {
                return I64_BINARY;
            }
            if (method.getReturnType() == boolean.class && parameterTypes.length == 2 && parameterTypes[0] == long.class && parameterTypes[1] == long.class) {
                return I64_COMPARISON;
            }
            if (method.getReturnType() == boolean.class && parameterTypes.length == 2 && parameterTypes[0] == boolean.class && parameterTypes[1] == boolean.class) {
                return BOOLEAN_BINARY;
            }
            throw new IllegalArgumentException("Unsupported scalar signature: " + method);
        }
    }
}
