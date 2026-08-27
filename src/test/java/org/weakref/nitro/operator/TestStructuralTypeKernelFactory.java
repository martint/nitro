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
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestStructuralTypeKernelFactory
{
    @Test
    void testDerivesStructKeySemanticsFromLogicalChildren()
    {
        TypeBinding scalar = Schema.unspecified(1).field(0).type();
        TypeBinding rowType = new TestingStructType(List.of(scalar, scalar));
        StructuralKeyKernel kernel = new StructuralTypeKernelFactory().key(rowType);

        StructVector rows = new StructVector(4);
        rows.setField("id", Streams.ofValuesAndNulls(
                new I64Vector(new long[] {11, 11, 11, 11}),
                new BooleanVector(new boolean[4])));
        rows.setField("code", Streams.ofValuesAndNulls(
                new I64Vector(new long[] {22, 22, 7, 9}),
                new BooleanVector(new boolean[] {false, false, true, true})));

        assertThat(kernel.identical(rows, null, 0, rows, null, 1)).isTrue();
        assertThat(kernel.hash(rows, null, 0)).isEqualTo(kernel.hash(rows, null, 1));
        assertThat(kernel.identical(rows, null, 0, rows, null, 2)).isFalse();
        assertThat(kernel.identical(rows, null, 2, rows, null, 3)).isTrue();
        assertThat(kernel.hash(rows, null, 2)).isEqualTo(kernel.hash(rows, null, 3));

        DictionaryVector dictionary = DictionaryVector.wrap(new int[] {2, 0}, rows);
        assertThat(kernel.identical(dictionary, null, 0, rows, null, 3)).isTrue();
        assertThat(kernel.hash(dictionary, null, 1)).isEqualTo(kernel.hash(rows, null, 0));
    }

    @Test
    void testDerivesArrayKeySemanticsFromElementBinding()
    {
        TypeBinding scalar = Schema.unspecified(1).field(0).type();
        StructuralKeyKernel kernel = new StructuralTypeKernelFactory().key(
                new TestingNestedType("array", ArrayVector.class, List.of(scalar)));

        ArrayVector arrays = new ArrayVector(4);
        System.arraycopy(new int[] {0, 2, 4, 6, 8}, 0, arrays.offsets(), 0, 5);
        arrays.setElements(Streams.ofValuesAndNulls(
                new I64Vector(new long[] {11, 22, 11, 22, 22, 11, 11, 99}),
                new BooleanVector(new boolean[] {false, true, false, true, true, false, false, true})));

        assertThat(kernel.identical(arrays, null, 0, arrays, null, 1)).isTrue();
        assertThat(kernel.hash(arrays, null, 0)).isEqualTo(kernel.hash(arrays, null, 1));
        assertThat(kernel.identical(arrays, null, 0, arrays, null, 2)).isFalse();
        assertThat(kernel.identical(arrays, null, 0, arrays, null, 3)).isTrue();

        DictionaryVector dictionary = DictionaryVector.wrap(new int[] {2, 0}, arrays);
        RleVector rle = new RleVector(new int[] {3}, DictionaryVector.wrap(new int[] {2}, arrays));
        assertThat(kernel.identical(rle, null, 0, arrays, null, 2)).isTrue();
        assertThat(kernel.hash(rle, null, 2)).isEqualTo(kernel.hash(arrays, null, 2));
    }

    @Test
    void testDerivesOrderIndependentMapKeySemanticsFromChildBindings()
    {
        TypeBinding scalar = Schema.unspecified(1).field(0).type();
        StructuralKeyKernel kernel = new StructuralTypeKernelFactory().key(
                new TestingNestedType("map", MapVector.class, List.of(scalar, scalar)));

        MapVector maps = new MapVector(3);
        System.arraycopy(new int[] {0, 2, 4, 6}, 0, maps.offsets(), 0, 4);
        maps.setEntries(
                Streams.ofValues(new I64Vector(new long[] {1, 2, 2, 1, 2, 1})),
                Streams.ofValuesAndNulls(
                        new I64Vector(new long[] {10, 20, 20, 10, 99, 10}),
                        new BooleanVector(new boolean[] {false, true, true, false, false, false})));

        assertThat(kernel.identical(maps, null, 0, maps, null, 1)).isTrue();
        assertThat(kernel.hash(maps, null, 0)).isEqualTo(kernel.hash(maps, null, 1));
        assertThat(kernel.identical(maps, null, 0, maps, null, 2)).isFalse();

        DictionaryVector dictionary = DictionaryVector.wrap(new int[] {1}, maps);
        RleVector rle = new RleVector(new int[] {4}, dictionary);
        assertThat(kernel.identical(rle, null, 3, maps, null, 0)).isTrue();
        assertThat(kernel.hash(rle, null, 0)).isEqualTo(kernel.hash(maps, null, 0));
    }

    private record TestingStructType(List<TypeBinding> nestedValueTypes)
            implements TypeBinding
    {
        private TestingStructType
        {
            nestedValueTypes = List.copyOf(nestedValueTypes);
        }

        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("testing:struct");
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
        public Set<Class<? extends Vector>> supportedVectorTypes()
        {
            return Set.of(StructVector.class, DictionaryVector.class);
        }
    }

    private record TestingNestedType(
            String name,
            Class<? extends Vector> physicalType,
            List<TypeBinding> nestedValueTypes)
            implements TypeBinding
    {
        private TestingNestedType
        {
            nestedValueTypes = List.copyOf(nestedValueTypes);
        }

        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("testing:" + name);
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
        public Set<Class<? extends Vector>> supportedVectorTypes()
        {
            return Set.of(physicalType, DictionaryVector.class, RleVector.class);
        }
    }
}
