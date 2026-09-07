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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestResolvedFixedWidthKeyLayout
{
    private static final StructuralKeyKernel STRUCTURAL_KEY = new StructuralKeyKernel()
    {
        @Override
        public boolean identical(Vector leftValues, Vector leftNulls, int leftPosition, Vector rightValues, Vector rightNulls, int rightPosition)
        {
            throw new AssertionError("semantic key fallback must not be called during fixed-width admission");
        }

        @Override
        public long hash(Vector values, Vector nulls, int position)
        {
            throw new AssertionError("semantic key fallback must not be called during fixed-width admission");
        }
    };

    @Test
    void testRejectsDeclaredCarrierThatDoesNotMatchPhysicalLane()
    {
        StructVector value = new StructVector(1);
        value.setField("lane", Streams.ofValues(new I64Vector(new long[] {1})));

        assertThatThrownBy(() -> resolve(
                value,
                new FixedWidthKeyLayout(List.of(FixedWidthKeyLayout.Lane.i32(List.of("lane"))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with the admitted vector");
    }

    @Test
    void testRejectsNullableStructuralLane()
    {
        StructVector value = new StructVector(1);
        value.setField("lane", Streams.ofValuesAndNulls(
                new I64Vector(new long[] {1}),
                new BooleanVector(new boolean[] {true})));

        assertThatThrownBy(() -> resolve(
                value,
                new FixedWidthKeyLayout(List.of(FixedWidthKeyLayout.Lane.i64(List.of("lane"))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("component is nullable");
    }

    @Test
    void testRejectsUnsupportedPhysicalRepresentation()
    {
        StructVector value = new StructVector(1);
        value.setField("lane", Streams.ofValues(new BinaryVector(1, 0)));

        assertThatThrownBy(() -> resolve(
                value,
                new FixedWidthKeyLayout(List.of(FixedWidthKeyLayout.Lane.i64(List.of("lane"))))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with the admitted vector");
    }

    @Test
    void testRejectsLaneCountBeyondGeneratedTableLimit()
    {
        StructVector value = new StructVector(1);
        value.setField("lane", Streams.ofValues(new I64Vector(new long[] {1})));
        List<FixedWidthKeyLayout.Lane> lanes = java.util.stream.IntStream.range(0, 9)
                .mapToObj(_ -> FixedWidthKeyLayout.Lane.i64(List.of("lane")))
                .toList();

        assertThatThrownBy(() -> resolve(value, new FixedWidthKeyLayout(lanes)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported fixed-width key lane count: 9");
    }

    private static ResolvedFixedWidthKeyLayout resolve(Vector value, FixedWidthKeyLayout layout)
    {
        TypeBinding type = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:invalid-fixed-width-layout");
            }

            @Override
            public Class<?> carrierType()
            {
                return Object.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<FixedWidthKeyLayout> fixedWidthKeyLayout()
            {
                return Optional.of(layout);
            }
        };
        return ResolvedFixedWidthKeyLayout.tryCreate(
                List.of(type),
                new StructuralKeyKernel[] {STRUCTURAL_KEY},
                new Vector[] {value});
    }
}
