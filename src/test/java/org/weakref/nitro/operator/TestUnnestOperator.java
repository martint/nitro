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
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.core.type.TypeVectorFactory;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.OperatorAssertions.operator;
import static org.weakref.nitro.data.Row.row;

public class TestUnnestOperator
{
    private static final TypeBinding BIGINT = new TypeBinding()
    {
        @Override
        public TypeIdentity identity()
        {
            return new TypeIdentity("testing:bigint");
        }

        @Override
        public Class<?> carrierType()
        {
            return long.class;
        }

        @Override
        public TypeOperators operators()
        {
            return TypeOperators.UNSPECIFIED;
        }

        @Override
        public Optional<TypeVectorFactory> vectorFactory()
        {
            return Optional.of(new TypeVectorFactory()
            {
                @Override
                public Vector constant(org.weakref.nitro.data.VectorAllocator allocator, Object value, int length)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Vector nullValues(org.weakref.nitro.data.VectorAllocator allocator, int length)
                {
                    return allocator.allocate(I64Vector.class, length, I64Vector::new);
                }
            });
        }
    };

    @Test
    void testExpandsArrayInBoundedBatches()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector arrays = array(new int[] {0, 2, 2, 5}, 10, 11, 30, 31, 32);
        Operator source = new TableOperator(
                Schema.unspecified(2),
                List.of(TableOperator.Page.values(
                        3,
                        new Vector[] {new I64Vector(new long[] {1, 2, 3}), arrays},
                        Mask.all(3))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[] {0},
                List.of(UnnestOperator.Mapping.direct(1, List.of(output))),
                Optional.empty(),
                false,
                new UnnestOperatorPolicy(2));

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(1L, 10L),
                row(1L, 11L),
                row(3L, 30L),
                row(3L, 31L),
                row(3L, 32L)));
    }

    @Test
    void testZipsArrayAndMapWithOuterPaddingAndOrdinality()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector arrays = array(new int[] {0, 2, 2}, 10, 11);
        MapVector maps = new MapVector(2);
        System.arraycopy(new int[] {0, 1, 1}, 0, maps.offsets(), 0, 3);
        maps.setEntries(
                Streams.ofValues(new I64Vector(new long[] {100})),
                Streams.ofValues(new I64Vector(new long[] {1_000})));
        Operator source = new TableOperator(
                Schema.unspecified(3),
                List.of(TableOperator.Page.values(
                        2,
                        new Vector[] {new I64Vector(new long[] {7, 8}), arrays, maps},
                        Mask.all(2))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[] {0},
                List.of(
                        UnnestOperator.Mapping.direct(1, List.of(output)),
                        UnnestOperator.Mapping.direct(2, List.of(output, output))),
                Optional.of(output),
                true,
                new UnnestOperatorPolicy(16));

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(7L, 10L, 100L, 1_000L, 1L),
                row(7L, 11L, null, null, 2L),
                row(8L, null, null, null, null)));
    }

    @Test
    void testOuterPaddingWithEmptyNullableElementStream()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector arrays = new ArrayVector(2);
        arrays.setElements(Streams.ofValuesAndNulls(new I64Vector(0), new BooleanVector(0)));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(new TableOperator.Page(
                        2,
                        new Streams[] {Streams.ofValuesAndNulls(arrays, new BooleanVector(new boolean[] {false, true}))},
                        Mask.all(2))));
        Field output = new Field(BIGINT, true);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output))),
                Optional.of(output),
                true,
                new UnnestOperatorPolicy(16));

        assertThat(operator(unnest)).matchesExactly(List.of(row(null, null), row(null, null)));
    }

    @Test
    void testPreservesDictionaryStructuralDomain()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector domain = array(new int[] {0, 2, 3}, 10, 11, 20);
        DictionaryVector arrays = DictionaryVector.wrap(new int[] {0, 0, 1, 1, 0}, domain);
        VectorAccess.RepeatedValues repeated = VectorAccess.repeatedValues(arrays);
        assertThat(repeated.supportsValueRuns()).isTrue();
        assertThat(repeated.valueRunCount(5)).isEqualTo(3);
        assertThat(repeated.valueRunEnd(0)).isEqualTo(2);
        assertThat(repeated.valueRunEnd(2)).isEqualTo(4);
        VectorAccess.RepeatedRangeLayout rangeLayout = repeated.rangeLayout();
        assertThat(rangeLayout.offsets()).isSameAs(domain.offsets());
        assertThat(rangeLayout.ids()).isSameAs(arrays.ids());
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        5,
                        new Vector[] {arrays},
                        Mask.all(5))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output))),
                Optional.empty(),
                false,
                new UnnestOperatorPolicy(16));

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(10L), row(11L),
                row(10L), row(11L),
                row(20L), row(20L),
                row(10L), row(11L)));
    }

    @Test
    void testExpandsRleArraysAcrossRunBoundaries()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector domain = array(new int[] {0, 2, 3}, 10, 11, 20);
        RleVector arrays = new RleVector(new int[] {2, 1}, domain);
        VectorAccess.RepeatedValues repeated = VectorAccess.repeatedValues(arrays);
        assertThat(repeated.supportsValueRuns()).isTrue();
        assertThat(repeated.valueRunCount(3)).isEqualTo(2);
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(3, new Vector[] {arrays}, Mask.all(3))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output))),
                Optional.empty(),
                false,
                new UnnestOperatorPolicy(16));

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(10L), row(11L),
                row(10L), row(11L),
                row(20L)));
    }

    @Test
    void testValueRunAdmissionUsesObservedRunDensity()
    {
        ArrayVector domain = array(new int[] {0, 1, 2}, 10, 20);
        int[] clusteredIds = new int[16];
        Arrays.fill(clusteredIds, 8, 16, 1);
        int[] alternatingIds = new int[16];
        for (int position = 0; position < alternatingIds.length; position++) {
            alternatingIds[position] = position & 1;
        }

        UnnestOperatorPolicy policy = new UnnestOperatorPolicy(16, 16, 4);
        assertThat(policy.admitsValueRuns(
                VectorAccess.repeatedValues(DictionaryVector.wrap(clusteredIds, domain)),
                clusteredIds.length)).isTrue();
        assertThat(policy.admitsValueRuns(
                VectorAccess.repeatedValues(DictionaryVector.wrap(alternatingIds, domain)),
                alternatingIds.length)).isFalse();
    }

    @Test
    void testRleArraysRespectCollectionNulls()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector value = array(new int[] {0, 2}, 10, 11);
        RleVector arrays = new RleVector(new int[] {3}, value);
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(new TableOperator.Page(
                        3,
                        new Streams[] {Streams.ofValuesAndNulls(arrays, new BooleanVector(new boolean[] {false, true, false}))},
                        Mask.all(3))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output))),
                Optional.empty(),
                false,
                new UnnestOperatorPolicy(16));

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(10L), row(11L),
                row(10L), row(11L)));
    }

    @Test
    void testFlattensArrayOfRowsAndInheritsRowNulls()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        StructVector rows = new StructVector(3);
        rows.setField("id", Streams.ofValues(new I64Vector(new long[] {10, 20, 30})));
        rows.setField("score", Streams.ofValues(new I64Vector(new long[] {100, 200, 300})));
        ArrayVector arrays = new ArrayVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, arrays.offsets(), 0, 3);
        arrays.setElements(Streams.ofValuesAndNulls(
                rows,
                new BooleanVector(new boolean[] {false, true, false})));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        2,
                        new Vector[] {arrays},
                        Mask.all(2))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(new UnnestOperator.Mapping(
                        0,
                        List.of(
                                new UnnestOperator.OutputMapping(0, List.of(0), output),
                                new UnnestOperator.OutputMapping(0, List.of(1), output)))),
                Optional.empty(),
                false,
                new UnnestOperatorPolicy(16));

        assertThat(operator(unnest)).matchesExactly(List.of(
                row(10L, 100L),
                row(null, null),
                row(30L, 300L)));
    }

    @Test
    void testFlattensArrayOfRowsWithoutPublishingInertRowNulls()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        StructVector rows = new StructVector(3);
        rows.setField("id", Streams.ofValues(new I64Vector(new long[] {10, 20, 30})));
        ArrayVector arrays = new ArrayVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, arrays.offsets(), 0, 3);
        arrays.setElements(Streams.ofValuesAndNulls(
                rows,
                new BooleanVector(new boolean[] {false, false, false})));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        2,
                        new Vector[] {arrays},
                        Mask.all(2))));
        Field output = Schema.unspecified(1).field(0);

        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(new UnnestOperator.Mapping(
                        0,
                        List.of(new UnnestOperator.OutputMapping(0, List.of(0), output)))),
                Optional.empty(),
                false,
                new UnnestOperatorPolicy(16));

        try (Batch batch = unnest.next()) {
            assertThat(batch.output(0).borrow(Stream.VALUES)).isSameAs(rows.field(0).values());
            assertThat(batch.output(0).borrowOrNull(Stream.NULLS)).isNull();
        }
        assertThat(unnest.hasNext()).isFalse();
        unnest.close();
    }

    @Test
    void testForwardsContiguousRepeatedChildren()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        I64Vector elements = new I64Vector(new long[] {10, 11, 20});
        ArrayVector arrays = new ArrayVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, arrays.offsets(), 0, 3);
        arrays.setElements(Streams.ofValues(elements));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(
                        2,
                        new Vector[] {arrays},
                        Mask.all(2))));
        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(Schema.unspecified(1).field(0)))),
                Optional.empty(),
                false,
                new UnnestOperatorPolicy(16));

        try (Batch batch = unnest.next()) {
            assertThat(batch.borrowMask().selectedCount()).isEqualTo(3);
            assertThat(batch.output(0).borrow(Stream.VALUES)).isSameAs(elements);
        }
        assertThat(unnest.hasNext()).isFalse();
        unnest.close();
    }

    @Test
    void testForwardsContiguousRepeatedChildrenWithOrdinality()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        I64Vector elements = new I64Vector(new long[] {10, 11, 20});
        ArrayVector arrays = new ArrayVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, arrays.offsets(), 0, 3);
        arrays.setElements(Streams.ofValues(elements));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(2, new Vector[] {arrays}, Mask.all(2))));
        Field output = Schema.unspecified(1).field(0);
        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output))),
                Optional.of(output),
                false,
                new UnnestOperatorPolicy(16));

        try (Batch batch = unnest.next()) {
            assertThat(batch.output(0).borrow(Stream.VALUES)).isSameAs(elements);
            Vector ordinality = batch.output(1).borrow(Stream.VALUES);
            VectorAccess.LongValues values = VectorAccess.longValues(ordinality);
            assertThat(new long[] {values.value(0), values.value(1), values.value(2)}).containsExactly(1, 2, 1);
        }
        unnest.close();
    }

    @Test
    void testPreservesLowCardinalityOrdinalityDomain()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        int rows = 32;
        ArrayVector arrays = new ArrayVector(rows);
        for (int row = 0; row <= rows; row++) {
            arrays.offsets()[row] = row * 2;
        }
        arrays.setElements(Streams.ofValues(new I64Vector(rows * 2)));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(rows, new Vector[] {arrays}, Mask.all(rows))));
        Field output = Schema.unspecified(1).field(0);
        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output))),
                Optional.of(output),
                false,
                new UnnestOperatorPolicy(128));

        try (Batch batch = unnest.next()) {
            Vector ordinality = batch.output(1).borrow(Stream.VALUES);
            assertThat(ordinality).isInstanceOf(DictionaryVector.class);
            DictionaryVector dictionary = (DictionaryVector) ordinality;
            assertThat(((I64Vector) dictionary.values()).values()).containsExactly(1, 2);
            assertThat(dictionary.ids()).startsWith(0, 1, 0, 1, 0, 1);
        }
        unnest.close();
    }

    @Test
    void testMappedOutputPreservesFixedWidthNestedDictionary()
    {
        DictionaryVector elements = DictionaryVector.wrap(
                new int[] {0, 1, 0},
                new I64Vector(new long[] {10, 20}));
        assertMappedOutput(
                Streams.ofValues(elements),
                new UnnestOperatorPolicy(16),
                elements,
                null,
                new int[] {0, 1, 0});
    }

    @Test
    void testMappedOutputPreservesVariableWidthNestedDictionary()
    {
        BinaryVector values = new BinaryVector(2, new int[] {0, 1, 2}, new byte[] {'a', 'b'});
        DictionaryVector elements = DictionaryVector.wrap(new int[] {0, 1, 0}, values);
        BooleanVector nullDomain = new BooleanVector(new boolean[] {false, true});
        DictionaryVector elementNulls = DictionaryVector.wrap(new int[] {0, 1, 0}, nullDomain);
        assertMappedOutput(
                Streams.of(elements, elementNulls, null),
                new UnnestOperatorPolicy(16),
                elements,
                elementNulls,
                new int[] {0, 1, 0});
    }

    @Test
    void testSiblingOutputsShareBorrowedMapping()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        I64Vector keyDomain = new I64Vector(new long[] {10, 20});
        DictionaryVector keys = DictionaryVector.wrap(new int[] {0, 1, 0}, keyDomain);
        BinaryVector valueDomain = new BinaryVector(2, new int[] {0, 1, 2}, new byte[] {'a', 'b'});
        DictionaryVector values = DictionaryVector.wrap(new int[] {0, 1, 0}, valueDomain);
        MapVector maps = new MapVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, maps.offsets(), 0, 3);
        maps.setEntries(Streams.ofValues(keys), Streams.ofValues(values));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(2, new Vector[] {maps}, Mask.all(2))));
        Field output = Schema.unspecified(1).field(0);
        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output, output))),
                Optional.of(output),
                false,
                new UnnestOperatorPolicy(16));

        try (Batch batch = unnest.next()) {
            DictionaryVector mappedKeys = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
            assertThat(mappedKeys).isSameAs(keys);
            assertThat(mappedKeys.ids()).startsWith(0, 1, 0);
            DictionaryVector mappedValues = (DictionaryVector) batch.output(1).borrow(Stream.VALUES);
            assertThat(mappedValues).isSameAs(values);
            assertThat(mappedValues.ids()).startsWith(0, 1, 0);
        }
        unnest.close();
    }

    @Test
    void testOuterSiblingOutputsPublishSharedExpansionMapping()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        MapVector maps = new MapVector(3);
        System.arraycopy(new int[] {0, 0, 0, 2}, 0, maps.offsets(), 0, 4);
        maps.setEntries(
                Streams.ofValues(new I64Vector(new long[] {10, 20})),
                Streams.ofValues(new I64Vector(new long[] {100, 200})));
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(new TableOperator.Page(
                        3,
                        new Streams[] {Streams.ofValuesAndNulls(maps, new BooleanVector(new boolean[] {true, false, false}))},
                        Mask.all(3))));
        Field output = Schema.unspecified(1).field(0);
        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(output, output))),
                Optional.empty(),
                true,
                new UnnestOperatorPolicy(16));

        try (Batch batch = unnest.next()) {
            DictionaryVector mappedKeys = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
            DictionaryVector mappedValues = (DictionaryVector) batch.output(1).borrow(Stream.VALUES);
            assertThat(mappedKeys.ids()).startsWith(0, 0, 0, 1);
            assertThat(mappedValues.ids()).startsWith(0, 0, 0, 1);
            assertThat(mappedKeys.hasSameRowMapping(mappedValues)).isTrue();
        }
        unnest.close();
    }

    private static void assertMappedOutput(Streams elements, UnnestOperatorPolicy policy, Vector expectedValues, Vector expectedNulls, int[] expectedIds)
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        ArrayVector arrays = new ArrayVector(2);
        System.arraycopy(new int[] {0, 2, 3}, 0, arrays.offsets(), 0, 3);
        arrays.setElements(elements);
        Operator source = new TableOperator(
                Schema.unspecified(1),
                List.of(TableOperator.Page.values(2, new Vector[] {arrays}, Mask.all(2))));
        Operator unnest = new UnnestOperator(
                allocator,
                source,
                new int[0],
                List.of(UnnestOperator.Mapping.direct(0, List.of(Schema.unspecified(1).field(0)))),
                Optional.of(Schema.unspecified(1).field(0)),
                false,
                policy);

        try (Batch batch = unnest.next()) {
            DictionaryVector mapped = (DictionaryVector) batch.output(0).borrow(Stream.VALUES);
            assertThat(mapped).isSameAs(expectedValues);
            assertThat(mapped.ids()).startsWith(expectedIds);
            if (expectedNulls != null) {
                DictionaryVector mappedNulls = (DictionaryVector) batch.output(0).borrow(Stream.NULLS);
                assertThat(mappedNulls).isSameAs(expectedNulls);
                assertThat(mappedNulls.ids()).startsWith(expectedIds);
            }
        }
        unnest.close();
    }

    private static ArrayVector array(int[] offsets, long... elements)
    {
        ArrayVector array = new ArrayVector(offsets.length - 1);
        System.arraycopy(offsets, 0, array.offsets(), 0, offsets.length);
        array.setElements(Streams.ofValues(new I64Vector(elements)));
        return array;
    }
}
