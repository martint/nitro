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
import org.weakref.nitro.core.type.PersistentKeyLayout;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDistinctKeySet
{
    private final EngineResources engineResources = EngineResources.createDefault();
    private final PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
    private final OperatorCodeGenerationResources codeGeneration = engineResources.operatorCodeGeneration();
    private final AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy = engineResources.operatorResources().adaptiveLongGroupingPolicy();
    private final FlatKeyTablePolicy flatKeyTablePolicy = engineResources.operatorResources().flatKeyTablePolicy();

    @Test
    void testFixedWidthStructuralDistinctUsesFlatPrimitiveLanes()
    {
        StructVector pairs = fixedWidthPairs(
                new long[] {1, 1, 2, 3, 2},
                new long[] {10, 10, 20, 30, 20});
        Vector[] values = {pairs};
        Vector[] nulls = {null};

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    List.of(fixedWidthPairType()),
                    allocator,
                    new Allocator.Context("fixed-width-flat-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[pairs.length()];
                int distinct = keys.addBatch(values, nulls, Mask.all(pairs.length()), positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2, 3);
                assertThat(keys.addBatch(values, nulls, Mask.all(pairs.length()), positions)).isZero();
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testCanonicalProjectionDistinctAcrossDictionaryAndRleShapes()
    {
        long instant100Zone1 = CanonicalFixedWidthKeyTestType.pack(100, 1);
        long instant100Zone2 = CanonicalFixedWidthKeyTestType.pack(100, 2);
        long instant200Zone3 = CanonicalFixedWidthKeyTestType.pack(200, 3);
        Vector dictionary = DictionaryVector.wrap(
                new int[] {0, 1, 2, 0},
                new I64Vector(new long[] {instant100Zone1, instant100Zone2, instant200Zone3}));

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    new Vector[] {dictionary},
                    List.of(CanonicalFixedWidthKeyTestType.INSTANCE),
                    allocator,
                    new Allocator.Context("canonical-projection-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[dictionary.length()];
                int distinct = keys.addBatch(
                        new Vector[] {dictionary},
                        new Vector[] {null},
                        Mask.all(dictionary.length()),
                        positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2);

                Vector rle = new RleVector(
                        new int[] {2, 1},
                        new I64Vector(new long[] {
                                CanonicalFixedWidthKeyTestType.pack(200, 9),
                                CanonicalFixedWidthKeyTestType.pack(300, 4)}));
                distinct = keys.addBatch(
                        new Vector[] {rle},
                        new Vector[] {null},
                        Mask.all(rle.length()),
                        positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(2);
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testProjectedFlatDistinctComposesDirectAndCanonicalFields()
    {
        Vector[] values = {
                utf8(new String[] {"alpha", "alpha", "beta", "beta", "ignored", "ignored"}),
                new I64Vector(new long[] {
                        CanonicalFixedWidthKeyTestType.pack(100, 1),
                        CanonicalFixedWidthKeyTestType.pack(100, 2),
                        CanonicalFixedWidthKeyTestType.pack(200, 3),
                        CanonicalFixedWidthKeyTestType.pack(200, 4),
                        CanonicalFixedWidthKeyTestType.pack(300, 5),
                        CanonicalFixedWidthKeyTestType.pack(300, 6)})};
        Vector[] nulls = {
                new BooleanVector(new boolean[] {false, false, false, false, true, true}),
                null};
        List<TypeBinding> types = List.of(rawBinaryType(), CanonicalFixedWidthKeyTestType.INSTANCE);

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    true,
                    types,
                    allocator,
                    new Allocator.Context("projected-flat-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[values[0].length()];
                int distinct = keys.addBatch(values, nulls, Mask.all(values[0].length()), positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2, 4);

                Vector[] wrappedValues = {
                        DictionaryVector.wrap(new int[] {0, 0, 1}, utf8(new String[] {"alpha", "gamma"})),
                        new RleVector(
                                new int[] {2, 1},
                                new I64Vector(new long[] {
                                        CanonicalFixedWidthKeyTestType.pack(100, 9),
                                        CanonicalFixedWidthKeyTestType.pack(300, 7)}))};
                distinct = keys.addBatch(
                        wrappedValues,
                        new Vector[] {null, null},
                        Mask.sparse(new int[] {0, 2}, 3),
                        positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(2);
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testProjectedFlatDistinctMapsOneNullableLogicalKeyToMultiplePhysicalLanes()
    {
        Vector[] values = {
                utf8(new String[] {"alpha", "alpha", "beta", "beta", "beta", "gamma"}),
                fixedWidthPairs(
                        new long[] {1, 1, 2, 2, 99, 99},
                        new long[] {10, 10, 20, 20, 999, 999})};
        Vector[] nulls = {
                null,
                new BooleanVector(new boolean[] {false, false, false, false, true, true})};

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    true,
                    List.of(rawBinaryType(), fixedWidthPairType()),
                    allocator,
                    new Allocator.Context("projected-flat-multi-lane-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[values[0].length()];
                int distinct = keys.addBatch(values, nulls, Mask.all(values[0].length()), positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2, 4, 5);

                Vector[] repeated = {
                        utf8(new String[] {"alpha", "beta", "delta"}),
                        fixedWidthPairs(
                                new long[] {1, 2, 7},
                                new long[] {10, 20, 70})};
                distinct = keys.addBatch(
                        repeated,
                        new Vector[] {null, new BooleanVector(new boolean[] {false, false, true})},
                        Mask.all(repeated[0].length()),
                        positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(2);
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testProjectedFlatDistinctComposesRecursiveProductLayout()
    {
        TypeBinding pairType = fixedWidthPairType();
        TypeBinding binaryType = rawBinaryType();
        StructVector rows = new StructVector(6);
        rows.setField("pair", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, fixedWidthPairs(
                        new long[] {1, 1, 9, 7, 8, 6},
                        new long[] {10, 10, 90, 70, 80, 60}))
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, true, false, true, false}))
                .build());
        rows.setField("label", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, utf8(new String[] {"a", "a", "unused", "ignored", "other", "arbitrary"}))
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, true, false, true, false}))
                .build());
        Vector[] values = {rows};
        Vector[] nulls = {new BooleanVector(new boolean[] {false, false, false, true, false, true})};

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    true,
                    List.of(productType(pairType, binaryType)),
                    allocator,
                    new Allocator.Context("recursive-product-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[rows.length()];
                int distinct = keys.addBatch(values, nulls, Mask.all(rows.length()), positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2, 3);

                int[] ids = {1, 0, 4, 2, 5, 3};
                Vector[] dictionaryValues = {DictionaryVector.ofTrustedIds(ids, rows)};
                Vector[] dictionaryNulls = {DictionaryVector.ofTrustedIds(ids, nulls[0])};
                assertThat(keys.addBatch(
                        dictionaryValues,
                        dictionaryNulls,
                        Mask.all(rows.length()),
                        positions))
                        .isZero();
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testFixedWidthStructuralDistinctComposesNestedDictionaryMappingsAndNullModes()
    {
        StructVector domain = fixedWidthPairs(
                new long[] {10, 20, 30},
                new long[] {100, 200, 300});
        int[] innerIds = {2, 0, 1, 2};
        int[] outerIds = {1, 3, 0, 2, 3, 1};
        DictionaryVector rows = DictionaryVector.wrapNested(
                outerIds,
                outerIds.length,
                DictionaryVector.wrapNested(innerIds, innerIds.length, domain));
        DictionaryVector rowNulls = nestedBooleanDictionary(
                outerIds,
                innerIds,
                new boolean[] {false, false, true});
        Mask sparse = Mask.sparse(new int[] {0, 1, 3, 5}, rows.length());

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet droppingNulls = createFixedWidthDistinct(rows, false, allocator, "fixed-width-nested-drop-null");
            try {
                int[] positions = new int[rows.length()];
                int distinct = droppingNulls.addBatch(
                        new Vector[] {rows},
                        new Vector[] {rowNulls},
                        sparse,
                        positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 3);
                assertThat(droppingNulls.addBatch(
                        new Vector[] {rows},
                        new Vector[] {rowNulls},
                        sparse,
                        positions)).isZero();
            }
            finally {
                droppingNulls.releaseBuffers();
            }

            DistinctKeySet retainingNulls = createFixedWidthDistinct(rows, true, allocator, "fixed-width-nested-retain-null");
            try {
                int[] positions = new int[rows.length()];
                int distinct = retainingNulls.addBatch(
                        new Vector[] {rows},
                        new Vector[] {rowNulls},
                        sparse,
                        positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 3);
                assertThat(retainingNulls.addBatch(
                        new Vector[] {rows},
                        new Vector[] {rowNulls},
                        sparse,
                        positions)).isZero();
            }
            finally {
                retainingNulls.releaseBuffers();
            }
        }
    }

    @Test
    void testGroupedFixedWidthStructuralDistinctIncludesGroupLane()
    {
        I64Vector groups = new I64Vector(new long[] {0, 0, 1, 1, 0, 1});
        StructVector pairs = fixedWidthPairs(
                new long[] {1, 1, 1, 2, 2, 1},
                new long[] {10, 10, 10, 20, 20, 10});
        Vector[] values = {groups, pairs};
        Vector[] nulls = {null, null};

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.createGroupedLong(
                    values,
                    List.of(fixedWidthPairType()),
                    allocator,
                    new Allocator.Context("grouped-fixed-width-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[groups.length()];
                int distinct = keys.addGroupedBatch(values, nulls, Mask.all(groups.length()), 2, positions);
                assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2, 3, 4);
                assertThat(keys.addGroupedBatch(values, nulls, Mask.all(groups.length()), 2, positions)).isZero();
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testSemanticStructuralDistinctIsRejectedWithoutPhysicalLayout()
    {
        StructVector rows = new StructVector(8);
        rows.setField("first", Streams.ofValues(new I64Vector(new long[] {1, 1, 2, 1, 2, 2, 1, 2})));
        rows.setField("second", Streams.ofValues(new I64Vector(new long[] {10, 10, 20, 10, 20, 20, 10, 20})));
        TypeBinding scalar = Schema.unspecified(1).field(0).type();
        TypeBinding rowType = new TestingStructType(List.of(scalar, scalar));

        try (Allocator allocator = new Allocator(engineResources)) {
            assertThatThrownBy(() -> DistinctKeySet.create(
                    new Vector[] {rows},
                    List.of(rowType),
                    allocator,
                    new Allocator.Context("structural-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("distinct")
                    .hasMessageContaining("direct physical or generated fixed-width")
                    .hasMessageContaining("ADR-0090");
        }
    }

    @Test
    void testGeneratedPhysicalFlatDistinctReportsFirstPositions()
    {
        int size = 256;
        String[] labels = new String[size];
        long[] ids = new long[size];
        for (int position = 0; position < size; position++) {
            labels[position] = "label-" + (position % 8);
            ids[position] = position;
        }
        int[] repeatedPositions = {65, 130, 195, 255};
        for (int position : repeatedPositions) {
            labels[position] = labels[position - 1];
            ids[position] = ids[position - 1];
        }
        Vector[] values = {utf8(labels), new I64Vector(ids)};
        Vector[] nulls = {null, null};

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    List.of(),
                    allocator,
                    new Allocator.Context("generated-flat-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[size];
                int expectedCount = size - repeatedPositions.length;
                assertThat(keys.addBatch(values, nulls, Mask.all(size), positions)).isEqualTo(expectedCount);
                assertThat(Arrays.copyOf(positions, expectedCount)).containsExactly(
                        java.util.stream.IntStream.range(0, size)
                                .filter(position -> Arrays.binarySearch(repeatedPositions, position) < 0)
                                .toArray());
                assertThat(keys.addBatch(values, nulls, Mask.all(size), positions)).isZero();
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testGeneratedPhysicalFlatDistinctConsumesSparsePositions()
    {
        int size = 512;
        String[] labels = new String[size];
        long[] first = new long[size];
        long[] second = new long[size];
        int[] selected = new int[size / 2];
        for (int position = 0; position < size; position++) {
            labels[position] = "label-" + (position % 8);
            first[position] = position % 16;
            second[position] = position / 16;
            if ((position & 1) == 0) {
                selected[position / 2] = position;
            }
        }
        // Both rows are selected; the generated sparse driver must report the first physical position.
        labels[258] = labels[256];
        first[258] = first[256];
        second[258] = second[256];
        Vector[] values = {utf8(labels), new I64Vector(first), new I64Vector(second)};
        Vector[] nulls = {null, null, null};

        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    List.of(),
                    allocator,
                    new Allocator.Context("generated-sparse-flat-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[size];
                int distinct = keys.addBatch(values, nulls, Mask.sparse(selected, size), positions);
                assertThat(distinct).isEqualTo(selected.length - 1);
                assertThat(Arrays.copyOf(positions, distinct))
                        .contains(256)
                        .doesNotContain(258);
                assertThat(keys.addBatch(values, nulls, Mask.sparse(selected, size), positions)).isZero();
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testFlatDistinctUsesProviderCanonicalLongStorage()
    {
        BinaryVector labels = new BinaryVector(3, 3);
        labels.setBytes(0, new byte[] {'a'});
        labels.setBytes(1, new byte[] {'b'});
        labels.setBytes(2, new byte[] {'a'});
        Vector[] values = {new I64Vector(new long[] {1, 2, 1}), labels};
        List<org.weakref.nitro.core.type.TypeBinding> types = List.of(
                CanonicalFlatKeyTestType.signedInteger(),
                Schema.unspecified(2).field(1).type());
        try (Allocator allocator = new Allocator(engineResources)) {
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    types,
                    allocator,
                    new Allocator.Context("canonical-flat-distinct"),
                    arrayPool,
                    codeGeneration,
                    DistinctKeySetPolicy.defaults(),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                int[] positions = new int[3];
                assertThat(keys.addBatch(values, new Vector[] {null, null}, Mask.all(3), positions)).isEqualTo(2);

                BinaryVector wideLabel = new BinaryVector(1, 1);
                wideLabel.setBytes(0, new byte[] {'c'});
                assertThatThrownBy(() -> keys.addBatch(
                        new Vector[] {new I64Vector(new long[] {1L << 40}), wideLabel},
                        new Vector[] {null, null},
                        Mask.all(1),
                        new int[1]))
                        .isInstanceOf(ArithmeticException.class);
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testPrimitiveRepresentationsReportAndReleaseRetainedMemory()
    {
        try (Allocator allocator = new Allocator(EngineResources.createDefault())) {
            for (int arity : new int[] {1, 2, 3, 5}) {
                Allocator.Context context = new Allocator.Context("distinct-" + arity);
                Vector[] values = new Vector[arity];
                Vector[] nulls = new Vector[arity];
                for (int column = 0; column < arity; column++) {
                    values[column] = new I64Vector(new long[] {column, column + 10, column + 20});
                    nulls[column] = new BooleanVector(new boolean[3]);
                }
                DistinctKeySet keys = DistinctKeySet.create(
                        values,
                        List.of(),
                        allocator,
                        context,
                        allocator.primitiveArrays(),
                        EngineResources.from(allocator).operatorCodeGeneration(),
                        DistinctKeySetPolicy.defaults(),
                        EngineResources.from(allocator).operatorResources().adaptiveLongGroupingPolicy(),
                        EngineResources.from(allocator).operatorResources().flatKeyTablePolicy());
                try {
                    assertThat(keys.addBatch(values, nulls, Mask.all(3), new int[3])).isEqualTo(3);
                    assertThat(allocator.currentBytes(context)).isPositive();
                }
                finally {
                    keys.releaseBuffers();
                }
                assertThat(allocator.currentBytes(context)).isZero();
            }

            Allocator.Context groupedContext = new Allocator.Context("grouped-distinct");
            Vector[] groupedValues = {
                    new I64Vector(new long[] {0, 0, 1}),
                    new I64Vector(new long[] {10, 20, 30})};
            Vector[] groupedNulls = {
                    new BooleanVector(new boolean[3]),
                    new BooleanVector(new boolean[3])};
            DistinctKeySet groupedKeys = DistinctKeySet.createGroupedLong(
                    groupedValues,
                    List.of(),
                    allocator,
                    groupedContext,
                    allocator.primitiveArrays(),
                    EngineResources.from(allocator).operatorCodeGeneration(),
                    DistinctKeySetPolicy.defaults(),
                    EngineResources.from(allocator).operatorResources().adaptiveLongGroupingPolicy(),
                    EngineResources.from(allocator).operatorResources().flatKeyTablePolicy());
            try {
                assertThat(groupedKeys.addGroupedBatch(groupedValues, groupedNulls, Mask.all(3), 2, new int[3])).isEqualTo(3);
                assertThat(allocator.currentBytes(groupedContext)).isPositive();
            }
            finally {
                groupedKeys.releaseBuffers();
            }
            assertThat(allocator.currentBytes(groupedContext)).isZero();
        }
    }

    @Test
    void testAdaptiveLongPairMigratesAndPromotesExactly()
    {
        Vector[] initialValues = longPair(new long[] {1, 2, 1, 99}, new long[] {10, 20, 10, 990});
        DistinctKeySet keys = DistinctKeySet.create(initialValues, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            assertDistinctPositions(keys, initialValues, new boolean[] {false, false, false, true}, 0, 1);
            assertDistinctPositions(keys, longPair(new long[] {2, 3, 3}, new long[] {20, 30, 30}), null, 1);
            assertDistinctPositions(keys, longPair(new long[] {4, 1}, new long[] {40, 10}), null, 0);

            // The fourth batch migrates the retained pair table into the generated compact representation.
            assertDistinctPositions(keys, longPair(new long[] {3, 5, 4}, new long[] {30, 50, 40}), null, 1);

            // A later value outside the signed-int compact domain promotes the complete retained set back to
            // the generated full-width representation without losing either old or new keys.
            long wide = 1L << 40;
            assertDistinctPositions(keys, longPair(new long[] {wide, 5, wide}, new long[] {60, 50, 60}), null, 0);
            assertDistinctPositions(keys, longPair(new long[] {1, 2, 3, 4, 5, wide}, new long[] {10, 20, 30, 40, 50, 60}), null);

            int[] firstIds = new int[16];
            int[] secondIds = new int[16];
            for (int position = 0; position < firstIds.length; position++) {
                firstIds[position] = (position >>> 1) & 1;
                secondIds[position] = position & 1;
            }
            assertDistinctPositions(keys, new Vector[] {
                    DictionaryVector.wrap(firstIds, new I64Vector(new long[] {1, 7})),
                    DictionaryVector.wrap(secondIds, new I64Vector(new long[] {10, 70}))}, null, 1, 2, 3);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testIndependentDictionaryPairDomainCachesPhysicalTuplesExactly()
    {
        int[] firstIds = new int[16];
        int[] secondIds = new int[16];
        for (int position = 0; position < firstIds.length; position++) {
            firstIds[position] = (position >>> 1) & 1;
            secondIds[position] = position & 1;
        }
        I64Vector secondDictionary = new I64Vector(new long[] {20, 21});
        Vector[] initial = {
                DictionaryVector.wrap(firstIds, new I64Vector(new long[] {10, 11})),
                DictionaryVector.wrap(secondIds, secondDictionary)};
        DistinctKeySet keys = DistinctKeySet.create(initial, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            assertDistinctPositions(keys, initial, null, 0, 1, 2, 3);
            assertDistinctPositions(keys, new Vector[] {
                    DictionaryVector.wrap(firstIds, ((DictionaryVector) initial[0]).values()),
                    DictionaryVector.wrap(secondIds, secondDictionary)}, null);

            // A changed physical dictionary invalidates the tuple cache, while the logical key table remains
            // authoritative and reports only tuples containing the genuinely new logical value.
            Vector[] changed = {
                    DictionaryVector.wrap(firstIds, new I64Vector(new long[] {10, 12})),
                    DictionaryVector.wrap(secondIds, secondDictionary)};
            assertDistinctPositions(keys, changed, null, 2, 3);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testIndependentDictionaryTupleDomainIsArityIndependent()
    {
        int length = 32;
        Vector[] values = new Vector[3];
        for (int field = 0; field < values.length; field++) {
            int[] ids = new int[length];
            for (int position = 0; position < length; position++) {
                ids[position] = (position >>> field) & 1;
            }
            values[field] = DictionaryVector.wrap(ids, new I64Vector(new long[] {field * 10L, field * 10L + 1}));
        }
        DistinctKeySet keys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            Vector[] nulls = new Vector[values.length];
            for (int field = 0; field < nulls.length; field++) {
                nulls[field] = new BooleanVector(new boolean[length]);
            }
            int[] positions = new int[length];
            int distinct = keys.addBatch(values, nulls, Mask.all(length), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
            assertThat(keys.addBatch(values, nulls, Mask.all(length), positions)).isZero();
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testFullWidthLongPairBatchStrategiesAreExact()
    {
        long wide = 1L << 40;
        for (boolean taggedHash : new boolean[] {false, true}) {
            Vector[] values = longPair(
                    new long[] {wide, 2, wide, 3, 4},
                    new long[] {10, 20, 10, 30, 40});
            DistinctKeySet keys = DistinctKeySet.create(
                    values,
                    arrayPool,
                    codeGeneration,
                    longPairPolicy(taggedHash),
                    adaptiveLongGroupingPolicy,
                    flatKeyTablePolicy);
            try {
                assertDistinctPositions(keys, values, new boolean[] {false, false, false, true, false}, 0, 1, 4);
                assertDistinctPositions(
                        keys,
                        longPair(new long[] {wide, 5, 6}, new long[] {10, 50, 60}),
                        null,
                        1,
                        2);
            }
            finally {
                keys.releaseBuffers();
            }
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctSharesDictionaryPositionOnlyForIdenticalMappings()
    {
        int[] innerIds = {2, 0, 1, 2};
        int[] outerIds = {0, 1, 2, 3, 0, 2};
        Vector[] shared = {
                nestedLongDictionary(outerIds, innerIds, new long[] {10, 20, 30}),
                nestedLongDictionary(outerIds, innerIds, new long[] {100, 200, 300}),
                nestedLongDictionary(outerIds, innerIds, new long[] {1_000, 2_000, 3_000})};
        DistinctKeySet keys = DistinctKeySet.create(shared, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            assertDistinctPositions(keys, shared, null, 0, 1, 2);

            // A content-equivalent but separately owned mapping must use the exact generic fallback. Changing only
            // the second lane's first mapping then creates one genuinely new tuple.
            Vector[] mismatched = {
                    nestedLongDictionary(outerIds, innerIds, new long[] {10, 20, 30}),
                    nestedLongDictionary(new int[] {1, 1, 2, 3, 0, 2}, innerIds, new long[] {100, 200, 300}),
                    nestedLongDictionary(outerIds, innerIds, new long[] {1_000, 2_000, 3_000})};
            assertDistinctPositions(keys, mismatched, null, 0);

            // A wide value can force compact-to-full promotion after the shared resolver was admitted. The replay
            // still consumes the ordered key accessors exactly and preserves all previously inserted tuples.
            int[] identity = {0, 1, 2};
            Vector[] promoted = {
                    nestedLongDictionary(identity, identity, new long[] {10, 1L << 40, 30}),
                    nestedLongDictionary(identity, identity, new long[] {100, 200, 300}),
                    nestedLongDictionary(identity, identity, new long[] {1_000, 2_000, 3_000})};
            assertDistinctPositions(keys, promoted, null, 1);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctSharesDictionaryPositionAcrossNullsExactly()
    {
        int[] identity = {0, 1, 2, 3};
        Vector[] values = {
                nestedLongDictionary(identity, identity, new long[] {1, 2, 3, 4}),
                nestedLongDictionary(identity, identity, new long[] {10, 20, 30, 40}),
                nestedLongDictionary(identity, identity, new long[] {100, 200, 300, 400})};

        DistinctKeySet nullFreeKeys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            Vector[] nulls = {
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length])};
            int[] positions = new int[identity.length];
            int distinct = nullFreeKeys.addBatch(values, nulls, Mask.all(identity.length), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 2, 3);
        }
        finally {
            nullFreeKeys.releaseBuffers();
        }

        DistinctKeySet nullableKeys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            Vector[] nulls = {
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[] {false, true, false, false}),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length])};
            int[] positions = new int[identity.length];
            int distinct = nullableKeys.addBatch(values, nulls, Mask.all(identity.length), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 2, 3);
        }
        finally {
            nullableKeys.releaseBuffers();
        }

        DistinctKeySet sparseKeys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            Vector[] nulls = {
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length]),
                    nestedBooleanDictionary(identity, identity, new boolean[] {false, true, false, false}),
                    nestedBooleanDictionary(identity, identity, new boolean[identity.length])};
            int[] positions = new int[identity.length];
            int distinct = sparseKeys.addBatch(values, nulls, Mask.sparse(new int[] {1, 3}, identity.length), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(3);
        }
        finally {
            sparseKeys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctRetainsNullsThroughCompactAndPromotedBatches()
    {
        Vector[] firstValues = {
                new I64Vector(new long[] {1, 1, 1, 1, 1, 2}),
                new I64Vector(new long[] {10, 99, 99, 10, 10, 20}),
                new I64Vector(new long[] {100, 100, 100, 100, 100, 200})};
        Vector[] firstNulls = {
                new BooleanVector(new boolean[] {false, false, false, true, true, false}),
                new BooleanVector(new boolean[] {false, true, true, false, false, false}),
                new BooleanVector(new boolean[6])};
        DistinctKeySet keys = DistinctKeySet.create(firstValues, true, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            int[] positions = new int[6];
            int distinct = keys.addBatch(firstValues, firstNulls, Mask.all(6), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 3, 5);

            long wide = 1L << 40;
            Vector[] promotedValues = {
                    new I64Vector(new long[] {1, 2, wide, wide}),
                    new I64Vector(new long[] {99, 99, 99, 99}),
                    new I64Vector(new long[] {100, 200, 300, 300})};
            Vector[] promotedNulls = {
                    new BooleanVector(new boolean[4]),
                    new BooleanVector(new boolean[] {true, true, true, true}),
                    new BooleanVector(new boolean[4])};
            positions = new int[4];
            distinct = keys.addBatch(promotedValues, promotedNulls, Mask.all(4), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(1, 2);
            assertThat(keys.addBatch(promotedValues, promotedNulls, Mask.all(4), positions)).isZero();
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctCachesStableSharedBasePositionsAcrossBatches()
    {
        int[] innerIds = {0, 1, 2};
        I64Vector[] valueBases = {
                new I64Vector(new long[] {1, 2, 3}),
                new I64Vector(new long[] {10, 20, 30}),
                new I64Vector(new long[] {100, 200, 300})};
        BooleanVector[] nullBases = {
                new BooleanVector(new boolean[] {false, false, false}),
                new BooleanVector(new boolean[] {false, false, false}),
                new BooleanVector(new boolean[] {false, false, false})};

        Vector[] firstValues = nestedLongDictionaries(new int[] {0, 1, 0}, innerIds, valueBases);
        Vector[] firstNulls = nestedBooleanDictionaries(new int[] {0, 1, 0}, innerIds, nullBases);
        DistinctKeySet keys = DistinctKeySet.create(firstValues, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            int[] positions = new int[3];
            int distinct = keys.addBatch(firstValues, firstNulls, Mask.all(3), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1);

            // Base position 1 was already submitted through a different logical row. Base position 2 is new and
            // must retain its first logical position from this batch.
            Vector[] secondValues = nestedLongDictionaries(new int[] {1, 2, 2}, innerIds, valueBases);
            Vector[] secondNulls = nestedBooleanDictionaries(new int[] {1, 2, 2}, innerIds, nullBases);
            distinct = keys.addBatch(secondValues, secondNulls, Mask.all(3), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(1);

            // Reusing a pooled base object under a new content generation invalidates the bitmap. Exact hashing
            // then observes the changed tuple rather than treating its physical position as already processed.
            valueBases[0].clearForReuse();
            valueBases[0].values()[0] = 99;
            Vector[] changedValues = nestedLongDictionaries(new int[] {0}, innerIds, valueBases);
            Vector[] changedNulls = nestedBooleanDictionaries(new int[] {0}, innerIds, nullBases);
            distinct = keys.addBatch(changedValues, changedNulls, Mask.all(1), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveMultiLongDistinctInvalidatesCachedNullBaseGeneration()
    {
        int[] identity = {0};
        I64Vector[] valueBases = {
                new I64Vector(new long[] {1}),
                new I64Vector(new long[] {10}),
                new I64Vector(new long[] {100})};
        BooleanVector changedNullBase = new BooleanVector(new boolean[] {true});
        BooleanVector[] nullBases = {
                new BooleanVector(new boolean[] {false}),
                changedNullBase,
                new BooleanVector(new boolean[] {false})};
        Vector[] values = nestedLongDictionaries(identity, identity, valueBases);
        Vector[] nulls = nestedBooleanDictionaries(identity, identity, nullBases);

        DistinctKeySet keys = DistinctKeySet.create(values, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            int[] positions = new int[1];
            assertThat(keys.addBatch(values, nulls, Mask.all(1), positions)).isZero();

            // clearForReuse advances the content generation and resets the physical null bit. The position that
            // was previously cached as null must therefore be submitted to the exact table in the new lifetime.
            changedNullBase.clearForReuse();
            values = nestedLongDictionaries(identity, identity, valueBases);
            nulls = nestedBooleanDictionaries(identity, identity, nullBases);
            int distinct = keys.addBatch(values, nulls, Mask.all(1), positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testDictionarySingleBinaryFastPathUsesActiveMaskFrequency()
    {
        int[] ids = new int[128];
        int[] nonEmptyPositions = new int[64];
        for (int position = 0; position < ids.length; position++) {
            ids[position] = position % 2;
            if (ids[position] != 0) {
                nonEmptyPositions[position / 2] = position;
            }
        }
        DictionaryVector dictionary = dictionary(new String[] {"", "value"}, ids);
        Vector[] values = {dictionary};
        Vector[] nulls = {new BooleanVector(new boolean[ids.length])};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, arrayPool, codeGeneration, flatKeyTablePolicy);
        assertThat(layout).isNotNull();

        layout.beginBatch(values, nulls);
        try {
            assertThat(layout.admitFrequentDictionarySentinel(values, Mask.all(ids.length))).isTrue();
            assertThat(layout.hasTrackedSentinel(values)).isTrue();
        }
        finally {
            layout.endBatch();
        }

        layout.releaseBuffers();

        layout = FlatKeyLayout.tryCreate(values, arrayPool, codeGeneration, flatKeyTablePolicy);
        assertThat(layout).isNotNull();
        layout.beginBatch(values, nulls);
        try {
            Mask filtered = Mask.sparse(nonEmptyPositions, ids.length);
            assertThat(layout.admitFrequentDictionarySentinel(values, filtered)).isFalse();
            assertThat(layout.hasTrackedSentinel(values)).isTrue();
        }
        finally {
            layout.endBatch();
            layout.releaseBuffers();
        }
    }

    @Test
    void testNullFreeSingleBinaryDictionaryUsesExactStableValueEquality()
    {
        DictionaryVector first = dictionary(
                new String[] {"", "alpha", "beta", "gamma"},
                new int[] {0, 1, 1, 2, 0, 3});
        DistinctKeySet keys = DistinctKeySet.create(new Vector[] {first}, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            int[] positions = new int[first.length()];
            int distinct = keys.addBatch(
                    new Vector[] {first},
                    new Vector[] {new BooleanVector(new boolean[first.length()])},
                    Mask.all(first.length()),
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 3, 5);

            // A new dictionary identity may assign different local ids. Equality must remain value-based across
            // batches while the physical fast path stores and probes query-stable ids internally.
            DictionaryVector second = dictionary(
                    new String[] {"beta", "delta", "", "alpha"},
                    new int[] {0, 1, 2, 3, 1});
            positions = new int[second.length()];
            distinct = keys.addBatch(
                    new Vector[] {second},
                    new Vector[] {new BooleanVector(new boolean[second.length()])},
                    Mask.all(second.length()),
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(1);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testEmptyBinarySentinelSurvivesNullableDictionaryTransition()
    {
        int[] firstIds = new int[128];
        for (int position = 0; position < firstIds.length; position++) {
            firstIds[position] = position % 2;
        }
        DictionaryVector first = dictionary(new String[] {"", "value"}, firstIds);
        DistinctKeySet keys = DistinctKeySet.create(
                new Vector[] {first},
                arrayPool,
                codeGeneration,
                DistinctKeySetPolicy.defaults(),
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            int[] positions = new int[first.length()];
            assertThat(keys.addBatch(
                    new Vector[] {first},
                    new Vector[] {new BooleanVector(new boolean[first.length()])},
                    Mask.all(first.length()),
                    positions)).isEqualTo(2);

            DictionaryVector nullable = dictionary(
                    new String[] {"value", ""},
                    new int[] {1, 0});
            assertThat(keys.addBatch(
                    new Vector[] {nullable},
                    new Vector[] {new BooleanVector(new boolean[] {false, true})},
                    Mask.all(nullable.length()),
                    positions)).isZero();
        }
        finally {
            keys.releaseBuffers();
        }
    }

    private static DictionaryVector dictionary(String[] entries, int[] ids)
    {
        int bytes = Arrays.stream(entries).mapToInt(String::length).sum();
        BinaryVector values = new BinaryVector(entries.length, bytes);
        for (int index = 0; index < entries.length; index++) {
            values.setBytes(index, entries[index].getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return DictionaryVector.wrap(ids, values);
    }

    private static Vector[] longPair(long[] first, long[] second)
    {
        return new Vector[] {new I64Vector(first), new I64Vector(second)};
    }

    private static DistinctKeySetPolicy longPairPolicy(boolean taggedHash)
    {
        DistinctKeySetPolicy defaults = DistinctKeySetPolicy.defaults();
        return new DistinctKeySetPolicy(
                defaults.pooledLongHashSetPolicy(),
                defaults.debugDistinctShapes(),
                defaults.sharedDictionaryPositionResolver(),
                defaults.sharedDictionaryNullResolver(),
                defaults.sharedDictionaryBasePositionCache(),
                defaults.adaptiveCompactMultiLong(),
                defaults.adaptiveRetainNullsBatch(),
                defaults.adaptiveCompactLongPair(),
                defaults.adaptiveCompactLongPairSampleSize(),
                defaults.adaptiveCompactMultiLongMinArity(),
                defaults.adaptivePagedLongBitmap(),
                defaults.pagedLongBitmapMinKeys(),
                defaults.pagedLongBitmapMaxBitsPerKey(),
                defaults.emptyBinaryFastPath(),
                defaults.filterSentinelBeforeHash(),
                defaults.adaptiveDirectBatch(),
                taggedHash,
                defaults.longPairNullFreeBatch(),
                defaults.adaptiveCompactLongPairStartBatch(),
                defaults.inlineSmallGroupedLong(),
                defaults.keyOnlyDictionaryDomain(),
                defaults.keyOnlyDictionaryDomainMinimumReduction(),
                defaults.keyOnlySparseRetentionMinPercent(),
                defaults.keyOnlyReservationHeadroomPercent(),
                defaults.keyOnlyMinimumReservation(),
                defaults.independentDictionaryTupleDomain(),
                defaults.independentDictionaryTupleDomainMinimumReduction(),
                defaults.independentDictionaryTupleDomainMaxEntries());
    }

    private static DictionaryVector nestedLongDictionary(int[] outerIds, int[] innerIds, long[] values)
    {
        return DictionaryVector.wrapNested(
                outerIds,
                outerIds.length,
                DictionaryVector.wrapNested(innerIds, innerIds.length, new I64Vector(values)));
    }

    private static DictionaryVector nestedBooleanDictionary(int[] outerIds, int[] innerIds, boolean[] values)
    {
        return DictionaryVector.wrapNested(
                outerIds,
                outerIds.length,
                DictionaryVector.wrapNested(innerIds, innerIds.length, new BooleanVector(values)));
    }

    private static Vector[] nestedLongDictionaries(int[] outerIds, int[] innerIds, I64Vector[] bases)
    {
        Vector[] vectors = new Vector[bases.length];
        for (int index = 0; index < vectors.length; index++) {
            vectors[index] = DictionaryVector.wrapNested(
                    outerIds,
                    outerIds.length,
                    DictionaryVector.wrapNested(innerIds, innerIds.length, bases[index]));
        }
        return vectors;
    }

    private static Vector[] nestedBooleanDictionaries(int[] outerIds, int[] innerIds, BooleanVector[] bases)
    {
        Vector[] vectors = new Vector[bases.length];
        for (int index = 0; index < vectors.length; index++) {
            vectors[index] = DictionaryVector.wrapNested(
                    outerIds,
                    outerIds.length,
                    DictionaryVector.wrapNested(innerIds, innerIds.length, bases[index]));
        }
        return vectors;
    }

    private static void assertDistinctPositions(
            DistinctKeySet keys,
            Vector[] values,
            boolean[] secondNulls,
            int... expectedPositions)
    {
        int length = values[0].length();
        Vector[] nulls = {
                new BooleanVector(new boolean[length]),
                new BooleanVector(secondNulls == null ? new boolean[length] : secondNulls)};
        int[] positions = new int[length];
        int distinct = keys.addBatch(values, nulls, Mask.all(length), positions);
        assertThat(Arrays.copyOf(positions, distinct)).containsExactly(expectedPositions);
    }

    @Test
    void testDenseBitmapBatchFallsBackExactlyWhenDomainBecomesSparse()
    {
        long[] denseKeys = new long[4_096];
        for (int index = 0; index < denseKeys.length; index++) {
            denseKeys[index] = index;
        }

        DistinctKeySet keys = DistinctKeySet.create(new Vector[] {new I64Vector(denseKeys)}, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            int[] positions = new int[denseKeys.length];
            assertThat(keys.addBatch(
                    new Vector[] {new I64Vector(denseKeys)},
                    new Vector[] {new BooleanVector(new boolean[denseKeys.length])},
                    Mask.all(denseKeys.length),
                    positions)).isEqualTo(denseKeys.length);

            // The first four distant pages make the paged representation too sparse. Conversion can happen in
            // the middle of this dense batch; the suffix must continue in the hash representation without losing
            // or duplicating the conversion-triggering key.
            long[] sparseSuffix = {4_096, 1L << 20, 2L << 20, 3L << 20, 4L << 20, 5L << 20, 1, 9_999};
            int[] suffixPositions = new int[sparseSuffix.length];
            int distinct = keys.addBatch(
                    new Vector[] {new I64Vector(sparseSuffix)},
                    new Vector[] {new BooleanVector(new boolean[sparseSuffix.length])},
                    Mask.all(sparseSuffix.length),
                    suffixPositions);
            assertThat(Arrays.copyOf(suffixPositions, distinct)).containsExactly(0, 1, 2, 3, 4, 5, 7);

            assertThat(keys.addBatch(
                    new Vector[] {new I64Vector(sparseSuffix)},
                    new Vector[] {new BooleanVector(new boolean[sparseSuffix.length])},
                    Mask.all(sparseSuffix.length),
                    suffixPositions)).isZero();
        }
        finally {
            keys.releaseBuffers();
        }
    }

    @Test
    void testLargeGroupedLongDomainPromotesInlineKeysExactly()
    {
        long[] groups = {0, 0, 0, 0, 0, 0, 0, 0};
        long[] values = {0, 1, 2, 3, 4, 5, 5, 99};
        boolean[] valueNulls = {false, false, false, false, false, false, false, true};
        DistinctKeySet keys = DistinctKeySet.createGroupedLong(new Vector[] {
                new I64Vector(groups),
                new I64Vector(values)}, arrayPool, codeGeneration, DistinctKeySetPolicy.defaults(), adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            int[] positions = new int[groups.length];
            int distinct = keys.addGroupedBatch(
                    new Vector[] {new I64Vector(groups), new I64Vector(values)},
                    new Vector[] {new BooleanVector(new boolean[groups.length]), new BooleanVector(valueNulls)},
                    Mask.all(groups.length),
                    (1 << 16) + 1,
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(0, 1, 2, 3, 4, 5);

            long[] laterValues = {0, 1, 5, 6};
            positions = new int[laterValues.length];
            distinct = keys.addGroupedBatch(
                    new Vector[] {new I64Vector(new long[laterValues.length]), new I64Vector(laterValues)},
                    new Vector[] {
                            new BooleanVector(new boolean[laterValues.length]),
                            new BooleanVector(new boolean[laterValues.length])},
                    Mask.all(laterValues.length),
                    (1 << 16) + 1,
                    positions);
            assertThat(Arrays.copyOf(positions, distinct)).containsExactly(3);
        }
        finally {
            keys.releaseBuffers();
        }
    }

    private static BinaryVector utf8(String[] values)
    {
        byte[][] encoded = Arrays.stream(values)
                .map(value -> value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .toArray(byte[][]::new);
        BinaryVector vector = new BinaryVector(values.length, Arrays.stream(encoded).mapToInt(value -> value.length).sum());
        for (int position = 0; position < encoded.length; position++) {
            vector.setBytes(position, encoded[position]);
        }
        return vector;
    }

    private DistinctKeySet createFixedWidthDistinct(Vector sample, boolean retainNulls, Allocator allocator, String context)
    {
        return DistinctKeySet.create(
                new Vector[] {sample},
                retainNulls,
                List.of(fixedWidthPairType()),
                allocator,
                new Allocator.Context(context),
                arrayPool,
                codeGeneration,
                DistinctKeySetPolicy.defaults(),
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
    }

    private static StructVector fixedWidthPairs(long[] high, long[] low)
    {
        StructVector pairs = new StructVector(high.length);
        pairs.setField("high", Streams.ofValues(new I64Vector(high)));
        pairs.setField("low", Streams.ofValues(new I64Vector(low)));
        return pairs;
    }

    private static TypeBinding fixedWidthPairType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:fixed-width-distinct-pair");
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
                return Optional.of(new FixedWidthKeyLayout(List.of(
                        FixedWidthKeyLayout.Lane.i64(List.of("high")),
                        FixedWidthKeyLayout.Lane.i64(List.of("low")))));
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class, DictionaryVector.class);
            }
        };
    }

    private static TypeBinding productType(TypeBinding pair, TypeBinding label)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:recursive-product-distinct-key");
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
            public List<TypeBinding> nestedValueTypes()
            {
                return List.of(pair, label);
            }

            @Override
            public Optional<PersistentKeyLayout> persistentKeyLayout()
            {
                return Optional.of(new PersistentKeyLayout(List.of(
                        new PersistentKeyLayout.Field(List.of("pair"), pair),
                        new PersistentKeyLayout.Field(List.of("label"), label))));
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class, DictionaryVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding rawBinaryType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:raw-binary-distinct-key");
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
            public boolean supportsRawKeyIdentity()
            {
                return true;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(BinaryVector.class, DictionaryVector.class, RleVector.class);
            }
        };
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
            return new TypeIdentity("testing:struct-distinct");
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
}
