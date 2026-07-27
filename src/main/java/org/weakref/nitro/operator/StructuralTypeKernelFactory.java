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
package org.weakref.nitro.operator;

import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Vector;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;

import static java.lang.invoke.MethodHandles.collectArguments;
import static java.util.Objects.requireNonNull;

/**
 * Engine-owned bridge from registry-supplied carrier operations to structural vector kernels.
 *
 * <p>Provider-private carrier classes are erased behind adapted method handles. Operators retain only the resulting
 * Nitro-vector kernel and never inspect a logical type identity, carrier class, or concrete vector representation.
 */
public final class StructuralTypeKernelFactory
{
    StructuralComparisonKernel comparison(TypeBinding type)
    {
        requireNonNull(type, "type is null");
        TypeOperators operators = requireNonNull(type.operators(), "type operators are null");
        boolean hasValueRead = operators.valueRead().isPresent();
        boolean hasComparison = operators.comparison().isPresent();
        boolean hasIdentical = operators.identical().isPresent();
        if (!hasValueRead && !hasComparison && !hasIdentical) {
            return LegacyStructuralComparisonKernel.INSTANCE;
        }
        if (!hasValueRead || !hasComparison || !hasIdentical) {
            throw new IllegalArgumentException("Type %s must provide valueRead, comparison, and identical together"
                    .formatted(type.identity()));
        }
        return new BoundStructuralComparisonKernel(
                type,
                operators.valueRead().orElseThrow(),
                operators.comparison().orElseThrow(),
                operators.identical().orElseThrow());
    }

    private static final class BoundStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        private static final MethodType STRUCTURAL_COMPARISON_TYPE =
                MethodType.methodType(int.class, Vector.class, int.class, Vector.class, int.class);
        private static final MethodType STRUCTURAL_IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle comparison;
        private final MethodHandle identical;

        private BoundStructuralComparisonKernel(
                TypeBinding type,
                MethodHandle valueRead,
                MethodHandle comparison,
                MethodHandle identical)
        {
            Class<?> carrier = type.carrierType();
            requireValueReadType(type, valueRead, carrier);
            requireType(type, "comparison", comparison, MethodType.methodType(int.class, carrier, carrier));
            requireType(type, "identical", identical, MethodType.methodType(boolean.class, carrier, carrier));
            MethodHandle structuralRead =
                    valueRead.asType(MethodType.methodType(carrier, Vector.class, int.class));
            this.comparison = bindBinary(structuralRead, comparison, STRUCTURAL_COMPARISON_TYPE);
            this.identical = bindBinary(structuralRead, identical, STRUCTURAL_IDENTICAL_TYPE);
        }

        @Override
        public int compare(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            boolean leftNull = OperatorVectorSupport.isNull(leftNulls, leftPosition);
            boolean rightNull = OperatorVectorSupport.isNull(rightNulls, rightPosition);
            if (leftNull || rightNull) {
                if (leftNull == rightNull) {
                    return 0;
                }
                return leftNull ? 1 : -1;
            }
            try {
                return (int) comparison.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural comparison failed", throwable);
            }
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            if (OperatorVectorSupport.isNull(leftNulls, leftPosition) ||
                    OperatorVectorSupport.isNull(rightNulls, rightPosition)) {
                return false;
            }
            try {
                return (boolean) identical.invokeExact(leftValues, leftPosition, rightValues, rightPosition);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural identity comparison failed", throwable);
            }
        }

        private static void requireType(TypeBinding type, String name, MethodHandle handle, MethodType expected)
        {
            requireNonNull(handle, name + " is null");
            if (!handle.type().equals(expected)) {
                throw new IllegalArgumentException("Type %s %s has signature %s; expected %s"
                        .formatted(type.identity(), name, handle.type(), expected));
            }
        }

        private static void requireValueReadType(TypeBinding type, MethodHandle valueRead, Class<?> carrier)
        {
            requireNonNull(valueRead, "valueRead is null");
            MethodType actual = valueRead.type();
            if (actual.parameterCount() != 2 ||
                    !Vector.class.isAssignableFrom(actual.parameterType(0)) ||
                    actual.parameterType(1) != int.class ||
                    actual.returnType() != carrier) {
                throw new IllegalArgumentException(
                        "Type %s valueRead has signature %s; expected (Vector, int)%s"
                                .formatted(type.identity(), actual, carrier.getTypeName()));
            }
        }

        private static MethodHandle bindBinary(
                MethodHandle structuralRead,
                MethodHandle operation,
                MethodType structuralType)
        {
            MethodHandle bound = collectArguments(operation, 0, structuralRead);
            bound = collectArguments(bound, 2, structuralRead);
            if (!bound.type().equals(structuralType)) {
                throw new IllegalStateException("Unexpected structural method-handle type: " + bound.type());
            }
            return bound;
        }
    }

    private enum LegacyStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        INSTANCE;

        @Override
        public boolean allowsLegacyPhysicalShortcuts()
        {
            return true;
        }

        @Override
        public int compare(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            return OperatorOrderingSemantics.compare(
                    leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition);
        }

        @Override
        public boolean identical(
                Vector leftValues,
                Vector leftNulls,
                int leftPosition,
                Vector rightValues,
                Vector rightNulls,
                int rightPosition)
        {
            return OperatorEqualitySemantics.equal(
                    leftValues, leftNulls, leftPosition, rightValues, rightNulls, rightPosition);
        }
    }
}
