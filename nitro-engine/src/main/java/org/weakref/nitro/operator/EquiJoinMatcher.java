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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;

import java.util.Optional;

final class EquiJoinMatcher
        implements JoinMatcher
{
    private final int[] outerJoinColumns;
    private final int[] innerJoinColumns;
    private final TypeBinding[] keyTypes;
    private final StructuralIdentityKernel[] identityKernels;
    private final Vector[] validatedOuterValues;
    private final Vector[] validatedInnerValues;

    EquiJoinMatcher(
            int outerJoinColumn,
            int innerJoinColumn,
            Schema outerSchema,
            Schema innerSchema,
            StructuralTypeKernelFactory structuralTypes)
    {
        this(
                new int[] {outerJoinColumn},
                new int[] {innerJoinColumn},
                outerSchema,
                innerSchema,
                structuralTypes);
    }

    EquiJoinMatcher(
            int[] outerJoinColumns,
            int[] innerJoinColumns,
            Schema outerSchema,
            Schema innerSchema,
            StructuralTypeKernelFactory structuralTypes)
    {
        if (outerJoinColumns.length != innerJoinColumns.length) {
            throw new IllegalArgumentException("Join key counts must match");
        }
        if (outerJoinColumns.length == 0) {
            throw new IllegalArgumentException("Equi-join requires at least one join key");
        }

        this.outerJoinColumns = outerJoinColumns.clone();
        this.innerJoinColumns = innerJoinColumns.clone();
        this.keyTypes = joinKeyTypes(outerSchema, this.outerJoinColumns, innerSchema, this.innerJoinColumns);
        this.identityKernels = new StructuralIdentityKernel[keyTypes.length];
        for (int keyIndex = 0; keyIndex < keyTypes.length; keyIndex++) {
            identityKernels[keyIndex] = structuralTypes.identity(keyTypes[keyIndex]);
        }
        this.validatedOuterValues = new Vector[keyTypes.length];
        this.validatedInnerValues = new Vector[keyTypes.length];
    }

    private static TypeBinding[] joinKeyTypes(
            Schema outerSchema,
            int[] outerJoinColumns,
            Schema innerSchema,
            int[] innerJoinColumns)
    {
        TypeBinding[] types = new TypeBinding[outerJoinColumns.length];
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            int index = keyIndex;
            Optional<TypeBinding> outerType = typeAt(outerSchema, outerJoinColumns[keyIndex]);
            Optional<TypeBinding> innerType = typeAt(innerSchema, innerJoinColumns[keyIndex]);
            if (outerType.filter(TypeBinding::isSpecified).isPresent() &&
                    innerType.filter(TypeBinding::isSpecified).isPresent() &&
                    !outerType.orElseThrow().identity().equals(innerType.orElseThrow().identity())) {
                throw new IllegalArgumentException("Nested-loop join key types do not match at index " + keyIndex);
            }
            types[keyIndex] = innerType.filter(TypeBinding::isSpecified)
                    .or(() -> outerType.filter(TypeBinding::isSpecified))
                    .or(() -> innerType)
                    .or(() -> outerType)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Nested-loop join key column is out of bounds at index " + index));
        }
        return types;
    }

    private static Optional<TypeBinding> typeAt(Schema schema, int column)
    {
        if (column < 0 || column >= schema.size()) {
            return Optional.empty();
        }
        return Optional.of(schema.field(column).type());
    }

    @Override
    public boolean producesFullCrossProduct()
    {
        return false;
    }

    @Override
    public boolean supportsPerPositionEmission()
    {
        return true;
    }

    @Override
    public boolean matches(Batch outerBatch, int outerPosition, BufferedJoinInput.InnerBatch innerBatch, int innerPosition)
    {
        for (int keyIndex = 0; keyIndex < outerJoinColumns.length; keyIndex++) {
            Output outerOutput = outerBatch.output(outerJoinColumns[keyIndex]);
            VectorAndNulls innerStreams = innerStreams(innerBatch, innerJoinColumns[keyIndex]);
            Vector outerValues = outerOutput.borrow(Stream.VALUES);
            validateVector(keyIndex, outerValues, validatedOuterValues, "outer");
            validateVector(keyIndex, innerStreams.values(), validatedInnerValues, "inner");
            if (!identityKernels[keyIndex].identical(
                    outerValues,
                    outerOutput.borrowOrNull(Stream.NULLS),
                    outerPosition,
                    innerStreams.values(),
                    innerStreams.nulls(),
                    innerBatch.sourcePosition(innerPosition))) {
                return false;
            }
        }
        return true;
    }

    private void validateVector(int keyIndex, Vector values, Vector[] validatedValues, String side)
    {
        if (validatedValues[keyIndex] == values) {
            return;
        }
        TypeBinding type = keyTypes[keyIndex];
        if (type.isSpecified() && !type.supportsVector(values)) {
            throw new IllegalArgumentException(
                    "Nested-loop join " + side + " key vector at index " + keyIndex +
                            " is incompatible with plan-time type " + type.identity());
        }
        validatedValues[keyIndex] = values;
    }

    private static VectorAndNulls innerStreams(BufferedJoinInput.InnerBatch innerBatch, int outputIndex)
    {
        if (innerBatch.retained()) {
            Output output = innerBatch.retainedBatch().output(outputIndex);
            return new VectorAndNulls(
                    output.borrow(Stream.VALUES),
                    output.borrowOrNull(Stream.NULLS));
        }

        Streams innerStreams = innerBatch.columns()[outputIndex];
        return new VectorAndNulls(
                innerStreams.values(),
                innerStreams.getOrNull(Stream.NULLS));
    }

    private record VectorAndNulls(Vector values, Vector nulls) {}
}
