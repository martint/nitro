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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
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
    StructuralIdentityKernel identity(TypeBinding type)
    {
        requireNonNull(type, "type is null");
        TypeOperators operators = requireNonNull(type.operators(), "type operators are null");
        if (operators.vectorIdentical().isPresent()) {
            return new DirectStructuralIdentityKernel(type, operators.vectorIdentical().orElseThrow());
        }
        boolean hasValueRead = operators.valueRead().isPresent();
        boolean hasIdentical = operators.identical().isPresent();
        if (!hasValueRead && !hasIdentical) {
            return LegacyStructuralIdentityKernel.INSTANCE;
        }
        if (!hasValueRead || !hasIdentical) {
            throw new IllegalArgumentException("Type %s must provide valueRead and identical together"
                    .formatted(type.identity()));
        }
        return new BoundStructuralIdentityKernel(
                type,
                operators.valueRead().orElseThrow(),
                operators.identical().orElseThrow());
    }

    StructuralKeyKernel key(TypeBinding type)
    {
        requireNonNull(type, "type is null");
        TypeOperators operators = requireNonNull(type.operators(), "type operators are null");
        boolean hasVectorHash = operators.vectorHash().isPresent();
        boolean hasVectorIdentical = operators.vectorIdentical().isPresent();
        if (hasVectorHash) {
            if (!hasVectorIdentical) {
                throw new IllegalArgumentException("Type %s must provide vectorHash and vectorIdentical together"
                        .formatted(type.identity()));
            }
            return new DirectStructuralKeyKernel(
                    type,
                    operators.vectorHash().orElseThrow(),
                    operators.vectorIdentical().orElseThrow());
        }
        boolean hasValueRead = operators.valueRead().isPresent();
        boolean hasHash = operators.hash().isPresent();
        boolean hasIdentical = operators.identical().isPresent();
        if (!hasValueRead && !hasHash && !hasIdentical) {
            if (type.supportedVectorTypes().contains(StructVector.class)) {
                return new StructStructuralKeyKernel(type.nestedValueTypes().stream()
                        .map(this::key)
                        .toArray(StructuralKeyKernel[]::new));
            }
            return LegacyStructuralKeyKernel.INSTANCE;
        }
        if (!hasValueRead || !hasHash || !hasIdentical) {
            throw new IllegalArgumentException("Type %s must provide valueRead, hash, and identical together"
                    .formatted(type.identity()));
        }
        return new BoundStructuralKeyKernel(
                type,
                operators.valueRead().orElseThrow(),
                operators.hash().orElseThrow(),
                operators.identical().orElseThrow());
    }

    /**
     * Structural semantics derived from a provider's ordered child bindings and Nitro's physical struct shape.
     * The engine does not inspect the provider's logical type identity or carrier class.
     */
    private static final class StructStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final int NULL_HASH = 0x9E37_79B9;

        private final StructuralKeyKernel[] fields;

        private StructStructuralKeyKernel(StructuralKeyKernel[] fields)
        {
            this.fields = fields.clone();
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            StructPosition row = structPosition(values, position);
            requireFieldCount(row.values(), fields.length);
            int hash = 1;
            for (int field = 0; field < fields.length; field++) {
                Streams streams = row.values().field(field);
                int fieldHash = OperatorVectorSupport.isNull(streams.getOrNull(Stream.NULLS), row.position())
                        ? NULL_HASH
                        : Long.hashCode(fields[field].hash(
                                streams.values(), streams.getOrNull(Stream.NULLS), row.position()));
                hash = 31 * hash + fieldHash;
            }
            return hash;
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
            StructPosition left = structPosition(leftValues, leftPosition);
            StructPosition right = structPosition(rightValues, rightPosition);
            requireFieldCount(left.values(), fields.length);
            requireFieldCount(right.values(), fields.length);
            for (int field = 0; field < fields.length; field++) {
                Streams leftField = left.values().field(field);
                Streams rightField = right.values().field(field);
                Vector leftFieldNulls = leftField.getOrNull(Stream.NULLS);
                Vector rightFieldNulls = rightField.getOrNull(Stream.NULLS);
                boolean leftNull = OperatorVectorSupport.isNull(leftFieldNulls, left.position());
                boolean rightNull = OperatorVectorSupport.isNull(rightFieldNulls, right.position());
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!fields[field].identical(
                        leftField.values(),
                        leftFieldNulls,
                        left.position(),
                        rightField.values(),
                        rightFieldNulls,
                        right.position())) {
                    return false;
                }
            }
            return true;
        }

        private static StructPosition structPosition(Vector values, int position)
        {
            return switch (values) {
                case StructVector struct -> new StructPosition(struct, position);
                case DictionaryVector dictionary -> structPosition(dictionary.values(), dictionary.ids()[position]);
                case RleVector rle -> structPosition(rle.values(), OperatorVectorSupport.runIndex(rle, position));
                default -> throw new IllegalArgumentException(
                        "Expected struct vector but found " + values.getClass().getSimpleName());
            };
        }

        private static void requireFieldCount(StructVector values, int expected)
        {
            if (values.fields().size() != expected) {
                throw new IllegalArgumentException("Struct field count does not match logical child binding count");
            }
        }

        private record StructPosition(StructVector values, int position) {}
    }

    private static final class BoundStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        private static final MethodType STRUCTURAL_IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle identical;

        private BoundStructuralIdentityKernel(TypeBinding type, MethodHandle valueRead, MethodHandle identical)
        {
            Class<?> carrier = type.carrierType();
            BoundStructuralComparisonKernel.requireValueReadType(type, valueRead, carrier);
            BoundStructuralComparisonKernel.requireType(
                    type, "identical", identical, MethodType.methodType(boolean.class, carrier, carrier));
            MethodHandle structuralRead =
                    valueRead.asType(MethodType.methodType(carrier, Vector.class, int.class));
            this.identical = BoundStructuralComparisonKernel.bindBinary(
                    structuralRead, identical, STRUCTURAL_IDENTICAL_TYPE);
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
    }

    private static final class DirectStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        private static final MethodType TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle identical;

        private DirectStructuralIdentityKernel(TypeBinding type, MethodHandle identical)
        {
            this.identical = requireDirectType(type, "vectorIdentical", identical, TYPE);
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
                throw new IllegalStateException("Direct structural identity comparison failed", throwable);
            }
        }
    }

    StructuralComparisonKernel comparison(TypeBinding type)
    {
        requireNonNull(type, "type is null");
        TypeOperators operators = requireNonNull(type.operators(), "type operators are null");
        boolean hasVectorComparison = operators.vectorComparison().isPresent();
        boolean hasVectorIdentical = operators.vectorIdentical().isPresent();
        if (hasVectorComparison) {
            if (!hasVectorIdentical) {
                throw new IllegalArgumentException("Type %s must provide vectorComparison and vectorIdentical together"
                        .formatted(type.identity()));
            }
            return new DirectStructuralComparisonKernel(
                    type,
                    operators.vectorComparison().orElseThrow(),
                    operators.vectorIdentical().orElseThrow());
        }
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

    private static final class BoundStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final MethodType STRUCTURAL_HASH_TYPE =
                MethodType.methodType(long.class, Vector.class, int.class);
        private static final MethodType STRUCTURAL_IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle hash;
        private final MethodHandle identical;

        private BoundStructuralKeyKernel(
                TypeBinding type,
                MethodHandle valueRead,
                MethodHandle hash,
                MethodHandle identical)
        {
            Class<?> carrier = type.carrierType();
            BoundStructuralComparisonKernel.requireValueReadType(type, valueRead, carrier);
            BoundStructuralComparisonKernel.requireType(
                    type, "hash", hash, MethodType.methodType(long.class, carrier));
            BoundStructuralComparisonKernel.requireType(
                    type, "identical", identical, MethodType.methodType(boolean.class, carrier, carrier));
            MethodHandle structuralRead =
                    valueRead.asType(MethodType.methodType(carrier, Vector.class, int.class));
            this.hash = collectArguments(hash, 0, structuralRead);
            if (!this.hash.type().equals(STRUCTURAL_HASH_TYPE)) {
                throw new IllegalStateException("Unexpected structural hash method-handle type: " + this.hash.type());
            }
            this.identical = BoundStructuralComparisonKernel.bindBinary(
                    structuralRead, identical, STRUCTURAL_IDENTICAL_TYPE);
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls, position)) {
                return 0;
            }
            try {
                return (long) hash.invokeExact(values, position);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Structural hash failed", throwable);
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
    }

    private static final class DirectStructuralKeyKernel
            implements StructuralKeyKernel
    {
        private static final MethodType HASH_TYPE =
                MethodType.methodType(long.class, Vector.class, int.class);
        private static final MethodType IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle hash;
        private final MethodHandle identical;

        private DirectStructuralKeyKernel(TypeBinding type, MethodHandle hash, MethodHandle identical)
        {
            this.hash = requireDirectType(type, "vectorHash", hash, HASH_TYPE);
            this.identical = requireDirectType(type, "vectorIdentical", identical, IDENTICAL_TYPE);
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            if (OperatorVectorSupport.isNull(nulls, position)) {
                return 0;
            }
            try {
                return (long) hash.invokeExact(values, position);
            }
            catch (Throwable throwable) {
                throw new IllegalStateException("Direct structural hash failed", throwable);
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
                throw new IllegalStateException("Direct structural identity comparison failed", throwable);
            }
        }
    }

    private static final class DirectStructuralComparisonKernel
            implements StructuralComparisonKernel
    {
        private static final MethodType COMPARISON_TYPE =
                MethodType.methodType(int.class, Vector.class, int.class, Vector.class, int.class);
        private static final MethodType IDENTICAL_TYPE =
                MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class);

        private final MethodHandle comparison;
        private final MethodHandle identical;

        private DirectStructuralComparisonKernel(TypeBinding type, MethodHandle comparison, MethodHandle identical)
        {
            this.comparison = requireDirectType(type, "vectorComparison", comparison, COMPARISON_TYPE);
            this.identical = requireDirectType(type, "vectorIdentical", identical, IDENTICAL_TYPE);
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
                throw new IllegalStateException("Direct structural comparison failed", throwable);
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
                throw new IllegalStateException("Direct structural identity comparison failed", throwable);
            }
        }
    }

    private static MethodHandle requireDirectType(
            TypeBinding type,
            String name,
            MethodHandle handle,
            MethodType expected)
    {
        if (!handle.type().equals(expected)) {
            throw new IllegalArgumentException("Type %s %s handle must have type %s, but is %s"
                    .formatted(type.identity(), name, expected, handle.type()));
        }
        return handle;
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

    private enum LegacyStructuralIdentityKernel
            implements StructuralIdentityKernel
    {
        INSTANCE;

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

    private enum LegacyStructuralKeyKernel
            implements StructuralKeyKernel
    {
        INSTANCE;

        @Override
        public boolean allowsLegacyPhysicalShortcuts()
        {
            return true;
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            return OperatorKeySemantics.hash(values, nulls, position);
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
        public PositionEquality bindPartitionEquality(
                Vector leftValues,
                Vector leftNulls,
                Vector rightValues,
                Vector rightNulls)
        {
            return OperatorEqualitySemantics.bindPartitionEquality(leftValues, leftNulls, rightValues, rightNulls);
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
