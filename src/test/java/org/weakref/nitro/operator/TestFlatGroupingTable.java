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
import org.weakref.nitro.core.type.LongFlatKeyStorage;
import org.weakref.nitro.core.type.PersistentKeyLayout;
import org.weakref.nitro.core.type.RepeatedKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFlatGroupingTable
{
    private static final int DEFAULT_VALUE_ID_CEILING = 65_536;
    private static final MethodHandle CANONICAL_DOUBLE = canonicalDoubleHandle();
    private static final LongFlatKeyStorage INTEGER_FLAT_KEY_STORAGE = new LongFlatKeyStorage()
    {
        @Override
        public int fixedSize()
        {
            return Integer.BYTES;
        }

        @Override
        public void write(byte[] target, int offset, long value)
        {
            int integer = toIntExact(value);
            target[offset] = (byte) integer;
            target[offset + 1] = (byte) (integer >>> 8);
            target[offset + 2] = (byte) (integer >>> 16);
            target[offset + 3] = (byte) (integer >>> 24);
        }

        @Override
        public long read(byte[] source, int offset)
        {
            return (source[offset] & 0xFF)
                    | (source[offset + 1] & 0xFF) << 8
                    | (source[offset + 2] & 0xFF) << 16
                    | source[offset + 3] << 24;
        }
    };

    private final EngineResources engineResources = EngineResources.createDefault();
    private final PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
    private final OperatorCodeGenerationResources codeGeneration = engineResources.operatorCodeGeneration();
    private final GroupingStateResources groupingResources = engineResources.groupingState();
    private final AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy = engineResources.operatorResources().adaptiveLongGroupingPolicy();
    private final FlatKeyTablePolicy flatKeyTablePolicy = engineResources.operatorResources().flatKeyTablePolicy();

    @Test
    void testInputNullProofsExpireWithBatchBinding()
    {
        Vector[] values = {new I64Vector(new long[] {10, 20})};
        Vector[] nonNull = {new RleVector(new int[] {2}, new BooleanVector(new boolean[] {false}))};
        Vector[] allNull = {new RleVector(new int[] {2}, new BooleanVector(new boolean[] {true}))};
        Vector[] mixedNulls = {new BooleanVector(new boolean[] {false, true})};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            for (Vector[] batchNulls : List.of(nonNull, allNull)) {
                layout.beginBatch(values, batchNulls);
                assertThat(layout.inputFieldNull(0, null, 0)).isEqualTo(batchNulls == allNull);
                assertThat(layout.inputFieldNull(0, null, 1)).isEqualTo(batchNulls == allNull);
                layout.endBatch();

                assertThat(layout.inputFieldNull(0, mixedNulls, 0)).isFalse();
                assertThat(layout.inputFieldNull(0, mixedNulls, 1)).isTrue();
                assertThat(layout.inputFieldNull(0, null, 1)).isFalse();
            }
            layout.beginBatch(values, allNull);
            layout.releaseBuffers();
            assertThat(layout.inputFieldNull(0, mixedNulls, 0)).isFalse();
            assertThat(layout.inputFieldNull(0, mixedNulls, 1)).isTrue();
        }
        finally {
            layout.endBatch();
            layout.releaseBuffers();
        }
    }

    @Test
    void testSpecializedLayoutHonorsBoundInputNullMetadata()
    {
        Vector[] values = {new I64Vector(new long[] {10, 20, 30, 40}), new I64Vector(new long[] {1, 2, 3, 4})};
        Vector[] nullable = {
                new RleVector(new int[] {2, 2}, new BooleanVector(new boolean[] {false, true})),
                DictionaryVector.wrap(new int[] {0, 1, 0, 1}, new BooleanVector(new boolean[] {true, false}))};
        Vector[] nonNull = {
                new RleVector(new int[] {4}, new BooleanVector(new boolean[] {false})),
                new BooleanVector(4)};
        FlatKeyLayout layout = BigintPairFlatKeyLayout.create(values, true, arrayPool, codeGeneration, flatKeyTablePolicy, List.of());
        try {
            assertThat(layout.inputFieldNull(0, nullable, 2)).isTrue();
            layout.beginBatch(values, nullable);
            assertThat(layout.inputFieldNull(0, null, 2)).isTrue();
            assertThat(layout.inputFieldNull(0, null, 0)).isFalse();
            assertThat(layout.inputFieldNull(1, null, 0)).isTrue();
            assertThat(layout.inputFieldNull(1, null, 1)).isFalse();
            layout.endBatch();
            assertThat(layout.inputFieldNull(1, nullable, 0)).isTrue();

            layout.beginBatch(values, nonNull);
            for (int position = 0; position < values[0].length(); position++) {
                assertThat(layout.inputFieldNull(0, null, position)).isFalse();
                assertThat(layout.inputFieldNull(1, null, position)).isFalse();
            }
            layout.endBatch();
            layout.beginBatch(values, nullable);
            assertThat(layout.inputFieldNull(0, null, 3)).isTrue();
            assertThat(layout.inputFieldNull(1, null, 2)).isTrue();
        }
        finally {
            layout.endBatch();
            layout.releaseBuffers();
        }
    }

    @Test
    void testStoredGroupNullMetadataAcrossEncodings()
    {
        Vector[] values = {new I64Vector(new long[] {10, 20, 30, 40}), new I64Vector(new long[] {1, 2, 3, 4})};
        Vector[] initialNulls = {
                new RleVector(new int[] {2, 2}, new BooleanVector(new boolean[] {false, false})),
                new RleVector(new int[] {4}, new BooleanVector(new boolean[] {false}))};
        Vector[] laterNulls = {
                new RleVector(new int[] {2, 2}, new BooleanVector(new boolean[] {false, true})),
                DictionaryVector.wrap(new int[] {0, 1, 0, 1}, new BooleanVector(new boolean[] {true, false}))};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        try {
            table.beginBatch(values, initialNulls);
            assertThat(table.assignGroup(values, initialNulls, 0, 0)).isZero();
            assertThat(table.fieldHasNull(0)).isFalse();
            assertThat(table.fieldHasNull(1)).isFalse();
            table.endBatch();

            table.beginBatch(values, laterNulls);
            assertThat(table.assignGroup(values, laterNulls, 1, 1)).isEqualTo(1);
            assertThat(table.fieldHasNull(0)).isFalse();
            assertThat(table.fieldHasNull(1)).isFalse();
            assertThat(table.assignGroup(values, laterNulls, 2, 2)).isEqualTo(2);
            assertThat(table.fieldHasNull(0)).isTrue();
            assertThat(table.fieldHasNull(1)).isTrue();
            assertThat(table.fieldNull(2, 0)).isTrue();
            assertThat(table.fieldNull(2, 1)).isTrue();
            table.endBatch();

            table.beginBatch(values, initialNulls);
            assertThat(table.assignGroup(values, initialNulls, 3, 3)).isEqualTo(3);
            assertThat(table.fieldHasNull(0)).isTrue();
            assertThat(table.fieldHasNull(1)).isTrue();
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testBinaryFlatKeysComposeRegionsWithDictionaryAndRuns()
    {
        BinaryVector base = utf8("padding", "alpha", "beta", "alpha", "tail");
        Vector region = new RegionVector(base, 1, 3);
        BinaryVector expected = utf8("alpha", "beta", "alpha");
        List<Vector> encodings = List.of(
                region,
                DictionaryVector.wrap(new int[] {2, 1, 0}, region),
                new RegionVector(DictionaryVector.wrap(new int[] {4, 1, 2, 3, 0}, base), 1, 3),
                new RleVector(new int[] {1, 1, 1}, region),
                new RegionVector(new RleVector(new int[] {1, 1, 1, 1, 1}, base), 1, 3),
                DictionaryVector.wrap(new int[] {2, 1, 0}, new RleVector(new int[] {1, 1, 1}, region)));
        for (Vector encoding : encodings) {
            for (int position = 0; position < expected.length(); position++) {
                assertThat(FlatTypeHandlers.BINARY.hashInput(encoding, position))
                        .isEqualTo(FlatTypeHandlers.BINARY.hashInput(expected, position));
            }
            Vector[] values = {encoding};
            Vector[] nulls = {null};
            FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
            FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
            try {
                table.beginBatch(values, nulls);
                assertThat(table.assignGroup(values, nulls, 0, 0)).isZero();
                assertThat(table.assignGroup(values, nulls, 1, 1)).isEqualTo(1);
                assertThat(table.assignGroup(values, nulls, 2, 2)).isZero();
                table.endBatch();

                // Stored keys must also compare equal through a different representation.
                Vector[] flat = {expected};
                table.beginBatch(flat, nulls);
                assertThat(table.assignGroup(flat, nulls, 0, 2)).isZero();
                assertThat(table.assignGroup(flat, nulls, 1, 2)).isEqualTo(1);
                table.endBatch();
            }
            finally {
                table.releaseBuffers();
            }
        }
    }

    @Test
    void testGeneratedProjectedFlatLayoutComposesVariableWidthAndCanonicalFields()
    {
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("projected-flat-grouping");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(rawBinaryType(), canonicalDoubleType()),
                allocator,
                context);
        try {
            Vector[] firstValues = {
                    utf8("alpha", "alpha", "beta", "beta", "skipped", "ignored"),
                    new F64Vector(new double[] {0.0, -0.0, 1.5, 1.5, 99.0, 8.0})};
            Vector[] firstNulls = {
                    new BooleanVector(new boolean[] {false, false, false, false, false, true}),
                    new BooleanVector(new boolean[] {false, false, false, false, false, true})};
            I64Vector firstGroups = new I64Vector(6);
            state.assignGroups(
                    firstValues,
                    firstNulls,
                    Mask.sparse(new int[] {0, 1, 2, 3, 5}, 6),
                    firstGroups);

            assertThat(firstGroups.values()[0]).isZero();
            assertThat(firstGroups.values()[1]).isZero();
            assertThat(firstGroups.values()[2]).isEqualTo(1);
            assertThat(firstGroups.values()[3]).isEqualTo(1);
            assertThat(firstGroups.values()[5]).isEqualTo(2);

            Vector[] wrappedValues = {
                    DictionaryVector.ofTrustedIds(new int[] {1, 0, 1, 2}, utf8("alpha", "beta", "new")),
                    new RegionVector(new F64Vector(new double[] {99.0, 1.5, -0.0, 1.5, 2.0}), 1, 4)};
            I64Vector wrappedGroups = new I64Vector(4);
            state.assignGroups(wrappedValues, new Vector[] {null, null}, Mask.all(4), wrappedGroups);

            assertThat(wrappedGroups.values()).containsExactly(1, 0, 1, 3);
            assertThat(state.groupCount()).isEqualTo(4);

            Vector[] runValues = {
                    DictionaryVector.ofTrustedIds(new int[] {0, 1, 0, 1}, utf8("alpha", "new")),
                    new RleVector(new int[] {2, 4}, new F64Vector(new double[] {-0.0, 2.0}))};
            I64Vector runGroups = new I64Vector(4);
            state.assignGroups(runValues, new Vector[] {null, null}, Mask.all(4), runGroups);

            assertThat(runGroups.values()).containsExactly(0, 4, 5, 3);
            assertThat(state.groupCount()).isEqualTo(6);

            Streams groupedBinary = state.groupedValues(0, Mask.all(6), null, allocator, context);
            assertThat(OperatorVectorSupport.binaryEquals(groupedBinary.values(), 0, "alpha".getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(OperatorVectorSupport.binaryEquals(groupedBinary.values(), 1, "beta".getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(((BooleanVector) groupedBinary.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values()[2]).isTrue();
            assertThat(OperatorVectorSupport.binaryEquals(groupedBinary.values(), 3, "new".getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(OperatorVectorSupport.binaryEquals(groupedBinary.values(), 4, "new".getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(OperatorVectorSupport.binaryEquals(groupedBinary.values(), 5, "alpha".getBytes(StandardCharsets.UTF_8))).isTrue();

            Streams groupedDouble = state.groupedValues(1, Mask.all(6), null, allocator, context);
            assertThat(Double.doubleToRawLongBits(((F64Vector) groupedDouble.values()).values()[0]))
                    .isEqualTo(Double.doubleToRawLongBits(0.0));
            assertThat(((F64Vector) groupedDouble.values()).values()[1]).isEqualTo(1.5);
            assertThat(((BooleanVector) groupedDouble.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values()[2]).isTrue();
            assertThat(((F64Vector) groupedDouble.values()).values()[3]).isEqualTo(2.0);
            assertThat(Double.doubleToRawLongBits(((F64Vector) groupedDouble.values()).values()[4]))
                    .isEqualTo(Double.doubleToRawLongBits(-0.0));
            assertThat(((F64Vector) groupedDouble.values()).values()[5]).isEqualTo(2.0);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGeneratedProjectedFlatLayoutSkipsUnboundCanonicalDiscriminator()
    {
        int size = 128;
        int[] sharedIds = new int[size];
        String[][] binaryFields = new String[4][size];
        double[] canonicalValues = new double[size];
        for (int position = 0; position < size; position++) {
            sharedIds[position] = position;
            binaryFields[0][position] = "customer-" + position;
            binaryFields[1][position] = "first";
            binaryFields[2][position] = "last";
            binaryFields[3][position] = "country";
            canonicalValues[position] = position;
        }

        Vector[] values = {
                nestedDictionary(sharedIds, binaryFields[0]),
                nestedDictionary(sharedIds, binaryFields[1]),
                nestedDictionary(sharedIds, binaryFields[2]),
                nestedDictionary(sharedIds, binaryFields[3]),
                DictionaryVector.wrapNested(
                        sharedIds,
                        size,
                        DictionaryVector.wrapNested(sharedIds, size, new F64Vector(canonicalValues)))};
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("projected-flat-canonical-discriminator");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(rawBinaryType(), rawBinaryType(), rawBinaryType(), rawBinaryType(), canonicalDoubleType()),
                allocator,
                context);
        try {
            I64Vector groups = new I64Vector(size);
            state.assignGroups(values, new Vector[] {null, null, null, null, null}, Mask.all(size), groups);

            assertThat(groups.values()).containsExactly(java.util.stream.LongStream.range(0, size).toArray());
            assertThat(state.groupCount()).isEqualTo(size);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGeneratedPersistentLayoutComposesRecursiveProductNullsAndMixedLeaves()
    {
        TypeBinding timestamp = fixedWidthPairType();
        TypeBinding label = rawBinaryType();
        TypeBinding rowType = productType(timestamp, label);

        StructVector timestampValues = new StructVector(6);
        timestampValues.setField("high", Streams.ofValues(new I64Vector(new long[] {10, 10, 99, 77, 88, 66})));
        timestampValues.setField("low", Streams.ofValues(new I64Vector(new long[] {1, 1, 9, 7, 8, 6})));
        StructVector rows = new StructVector(6);
        rows.setField("timestamp", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, timestampValues)
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, true, false, true, false}))
                .build());
        rows.setField("label", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, utf8("a", "a", "unused", "ignored", "other", "arbitrary"))
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, true, false, true, false}))
                .build());
        Vector[] values = {rows};
        Vector[] nulls = {new BooleanVector(new boolean[] {false, false, false, true, false, true})};

        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("recursive-product-grouping");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(rowType),
                allocator,
                context);
        try {
            I64Vector groups = new I64Vector(rows.length());
            state.assignGroups(values, nulls, Mask.all(rows.length()), groups);
            assertThat(groups.values()).containsExactly(0, 0, 1, 2, 1, 2);
            assertThat(state.groupCount()).isEqualTo(3);

            Streams grouped = state.groupedValues(0, Mask.all(3), null, allocator, context);
            assertThat(grouped.values()).isInstanceOf(StructVector.class);
            assertThat(((BooleanVector) grouped.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values())
                    .containsExactly(false, false, true);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGeneratedPersistentLayoutConsumesRecursiveProductDictionaryDomain()
    {
        TypeBinding timestamp = fixedWidthPairType();
        TypeBinding label = rawBinaryType();
        TypeBinding rowType = productType(timestamp, label);

        StructVector timestampValues = new StructVector(4);
        timestampValues.setField("high", Streams.ofValues(new I64Vector(new long[] {10, 10, 99, 77})));
        timestampValues.setField("low", Streams.ofValues(new I64Vector(new long[] {1, 1, 9, 7})));
        StructVector domain = new StructVector(4);
        domain.setField("timestamp", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, timestampValues)
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, true, false}))
                .build());
        domain.setField("label", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, utf8("same", "same", "ignored", "unused"))
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, true, false}))
                .build());
        int[] ids = {0, 2, 0, 1, 2, 2};
        DictionaryVector keys = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                ids.length,
                domain,
                new int[] {2, 1, 3, 0});
        int[] counts = new int[5];
        int[] groups = new int[5];
        int[] representatives = new int[5];

        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("recursive-product-dictionary-domain");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(rowType),
                allocator,
                context);
        try {
            state.initializeSchema(new Vector[] {keys}, new Vector[] {null}, Mask.all(ids.length));
            assertThat(state.assignSingleDictionaryDomain(
                    keys,
                    null,
                    Mask.all(ids.length),
                    counts,
                    groups,
                    representatives))
                    .isEqualTo(5);
            assertThat(counts).containsExactly(2, 1, 3, 0, 0);
            assertThat(groups[0]).isZero();
            assertThat(groups[1]).isZero();
            assertThat(groups[2]).isEqualTo(1);
            assertThat(state.groupCount()).isEqualTo(2);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGeneratedRepeatedLayoutConsumesOnlyUsedDictionaryDomainPositions()
    {
        TypeBinding arrayType = orderedRepeatedType(rawLongType());
        ArrayVector domain = new ArrayVector(5);
        System.arraycopy(new int[] {0, 2, 4, 6, 8, 10}, 0, domain.offsets(), 0, 6);
        domain.setElements(Streams.ofValues(new I64Vector(new long[] {
                1, 2,
                9, 9,
                1, 2,
                3, 4,
                7, 8})));
        int[] ids = {0, 2, 0, 3, 2, 3};
        DictionaryVector keys = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                ids.length,
                domain,
                new int[] {2, 0, 2, 2, 0});
        int[] counts = new int[6];
        int[] groups = new int[6];
        int[] representatives = new int[6];

        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("repeated-key-dictionary-domain");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(arrayType),
                allocator,
                context);
        try {
            state.initializeSchema(new Vector[] {keys}, new Vector[] {null}, Mask.all(ids.length));
            assertThat(state.assignSingleDictionaryDomain(
                    keys,
                    null,
                    Mask.all(ids.length),
                    counts,
                    groups,
                    representatives))
                    .isEqualTo(6);
            assertThat(counts).containsExactly(2, 0, 2, 2, 0, 0);
            assertThat(groups[0]).isZero();
            assertThat(groups[2]).isZero();
            assertThat(groups[3]).isEqualTo(1);
            assertThat(state.groupCount()).isEqualTo(2);

            Streams grouped = state.groupedValues(0, Mask.all(2), null, allocator, context);
            assertThat(grouped.values()).isInstanceOf(ArrayVector.class);
            ArrayVector groupedArrays = (ArrayVector) grouped.values();
            assertThat(groupedArrays.offsets()).startsWith(0, 2, 4);
            assertThat(((I64Vector) groupedArrays.elements().values()).values()).startsWith(1, 2, 3, 4);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGeneratedRepeatedLayoutReusesLongAdjacentRuns()
    {
        int positions = 300;
        int firstRun = 256;
        ArrayVector arrays = new ArrayVector(positions);
        long[] elements = new long[firstRun * 2 + (positions - firstRun)];
        int element = 0;
        for (int position = 0; position < positions; position++) {
            arrays.offsets()[position] = element;
            if (position < firstRun) {
                elements[element++] = 1;
                elements[element++] = 2;
            }
            else {
                elements[element++] = 3;
            }
        }
        arrays.offsets()[positions] = element;
        arrays.setElements(Streams.ofValues(new I64Vector(elements)));

        TypeBinding arrayType = orderedRepeatedType(rawLongType());
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("repeated-key-run-reuse");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(arrayType),
                allocator,
                context);
        try {
            I64Vector groups = new I64Vector(positions);
            state.assignGroups(new Vector[] {arrays}, new Vector[] {null}, Mask.all(positions), groups);
            assertThat(groups.values()).startsWith(new long[firstRun]);
            assertThat(Arrays.copyOfRange(groups.values(), firstRun, positions))
                    .containsOnly(1);
            assertThat(state.groupCount()).isEqualTo(2);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testOrderedRepeatedLongGroupingPreservesOrderNullsEmptyAndOuterMappings()
    {
        TypeBinding elementType = rawLongType();
        TypeBinding arrayType = orderedRepeatedType(elementType);
        ArrayVector domain = new ArrayVector(6);
        int[] offsets = {0, 2, 4, 6, 8, 8, 8};
        System.arraycopy(offsets, 0, domain.offsets(), 0, offsets.length);
        domain.setElements(Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, new I64Vector(new long[] {1, 2, 1, 2, 1, 3, 0, 2}))
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, false, false, false, false, true, false}))
                .build());

        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("ordered-repeated-grouping");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(arrayType),
                allocator,
                context);
        try {
            I64Vector groups = new I64Vector(domain.length());
            state.assignGroups(
                    new Vector[] {domain},
                    new Vector[] {new BooleanVector(new boolean[] {false, false, false, false, false, true})},
                    Mask.all(domain.length()),
                    groups);
            assertThat(groups.values()).containsExactly(0, 0, 1, 2, 3, 4);
            assertThat(state.groupCount()).isEqualTo(5);

            DictionaryVector dictionary = new DictionaryVector(new int[] {2, 0, 3, 4, 1}, domain);
            I64Vector dictionaryGroups = new I64Vector(dictionary.length());
            state.assignGroups(new Vector[] {dictionary}, new Vector[] {null}, Mask.all(dictionary.length()), dictionaryGroups);
            assertThat(dictionaryGroups.values()).containsExactly(1, 0, 2, 3, 0);
            assertThat(state.groupCount()).isEqualTo(5);

            Streams grouped = state.groupedValues(0, Mask.all(5), null, allocator, context);
            assertThat(grouped.values()).isInstanceOf(ArrayVector.class);
            assertThat(((BooleanVector) grouped.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values())
                    .containsExactly(false, false, false, false, true);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGeneratedOrderedRepeatedKernelsCoverEveryDirectStorageKind()
    {
        assertOrderedRepeatedGroups(
                new I32Vector(new int[] {1, 2, 1, 2, 2, 1}),
                rawRepeatedElementType("testing:repeated-i32", long.class, Set.of(I32Vector.class)));
        assertOrderedRepeatedGroups(
                new BooleanVector(new boolean[] {false, true, false, true, true, false}),
                rawRepeatedElementType("testing:repeated-boolean", boolean.class, Set.of(BooleanVector.class)));
        assertOrderedRepeatedGroups(
                new F64Vector(new double[] {Double.NaN, -0.0, Double.NaN, -0.0, -0.0, Double.NaN}),
                rawRepeatedElementType("testing:repeated-f64", double.class, Set.of(F64Vector.class)));
        assertOrderedRepeatedGroups(
                utf8("a", "b", "a", "b", "b", "a"),
                rawRepeatedElementType("testing:repeated-binary", Object.class, Set.of(BinaryVector.class)));
    }

    @Test
    void testGeneratedOrderedRepeatedProductGroupingPreservesProductAndLeafNullBoundaries()
    {
        TypeBinding rowType = productType(rawLongType(), rawBinaryType());
        StructVector rows = new StructVector(6);
        rows.setField("timestamp", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, new I64Vector(new long[] {1, 1, 1, 0, 0, 1}))
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, false, false, true, false}))
                .build());
        rows.setField("label", Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, utf8("a", "a", "ignored", "ignored", "ignored", "a"))
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, true, false, true, false}))
                .build());

        ArrayVector arrays = new ArrayVector(6);
        System.arraycopy(new int[] {0, 1, 2, 3, 4, 5, 6}, 0, arrays.offsets(), 0, 7);
        arrays.setElements(Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, rows)
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(new boolean[] {false, false, false, true, false, false}))
                .build());

        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("ordered-repeated-product-grouping");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(orderedRepeatedType(rowType)),
                allocator,
                context);
        try {
            I64Vector groups = new I64Vector(arrays.length());
            state.assignGroups(new Vector[] {arrays}, new Vector[] {null}, Mask.all(arrays.length()), groups);
            assertThat(groups.values()).containsExactly(0, 0, 1, 2, 3, 0);
            assertThat(state.groupCount()).isEqualTo(4);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    private void assertOrderedRepeatedGroups(Vector elements, TypeBinding elementType)
    {
        ArrayVector arrays = new ArrayVector(3);
        System.arraycopy(new int[] {0, 2, 4, 6}, 0, arrays.offsets(), 0, 4);
        arrays.setElements(Streams.ofValues(elements));
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("generated-repeated-storage-kind");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(orderedRepeatedType(elementType)),
                allocator,
                context);
        try {
            I64Vector groups = new I64Vector(3);
            state.assignGroups(new Vector[] {arrays}, new Vector[] {null}, Mask.all(3), groups);
            assertThat(groups.values()).containsExactly(0, 0, 1);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testUnorderedRepeatedMapGroupingCanonicalizesEntriesAndPreservesMultiplicityNullsAndWrappers()
    {
        TypeBinding mapType = unorderedRepeatedMapType();
        MapVector domain = maps(
                new long[][] {{1, 2}, {2, 1}, {1, 2}, {}, {1, 1}, {1}, {3}, {3}, {0}, {0x1_0000_0001L}, {}},
                new String[][] {{"a", "b"}, {"b", "a"}, {"x", "b"}, {}, {"a", "a"}, {"a"}, {"ignored-a"}, {"ignored-b"}, {"collision"}, {"collision"}, {}},
                new boolean[][] {{false, false}, {false, false}, {false, false}, {}, {false, false}, {false}, {true}, {true}, {false}, {false}, {}});

        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("unordered-repeated-grouping");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(mapType),
                allocator,
                context);
        try {
            I64Vector groups = new I64Vector(domain.length());
            state.assignGroups(
                    new Vector[] {domain},
                    new Vector[] {new BooleanVector(new boolean[] {false, false, false, false, false, false, false, false, false, false, true})},
                    Mask.all(domain.length()),
                    groups);
            assertThat(groups.values()).containsExactly(0, 0, 1, 2, 3, 4, 5, 5, 6, 7, 8);
            assertThat(state.groupCount()).isEqualTo(9);

            Vector region = new RegionVector(domain, 1, 3);
            I64Vector regionGroups = new I64Vector(region.length());
            state.assignGroups(new Vector[] {region}, new Vector[] {null}, Mask.all(region.length()), regionGroups);
            assertThat(regionGroups.values()).containsExactly(0, 1, 2);

            Vector dictionary = DictionaryVector.wrap(new int[] {7, 0, 4, 3, 1}, domain);
            I64Vector dictionaryGroups = new I64Vector(dictionary.length());
            state.assignGroups(new Vector[] {dictionary}, new Vector[] {null}, Mask.all(dictionary.length()), dictionaryGroups);
            assertThat(dictionaryGroups.values()).containsExactly(5, 0, 3, 2, 0);

            Vector rle = new RleVector(
                    new int[] {2, 3},
                    DictionaryVector.wrap(new int[] {2, 6}, domain));
            I64Vector rleGroups = new I64Vector(rle.length());
            state.assignGroups(new Vector[] {rle}, new Vector[] {null}, Mask.all(rle.length()), rleGroups);
            assertThat(rleGroups.values()).containsExactly(1, 1, 5, 5, 5);

            MapVector dictionaryChildren = new MapVector(2);
            System.arraycopy(new int[] {0, 2, 4}, 0, dictionaryChildren.offsets(), 0, 3);
            dictionaryChildren.setEntries(
                    Streams.ofValues(DictionaryVector.wrap(new int[] {0, 1, 1, 0}, new I64Vector(new long[] {11, 12}))),
                    Streams.builder()
                            .put(org.weakref.nitro.data.Stream.VALUES, DictionaryVector.wrap(new int[] {0, 1, 1, 0}, utf8("left", "right")))
                            .put(org.weakref.nitro.data.Stream.NULLS, DictionaryVector.wrap(new int[] {0, 0, 0, 0}, new BooleanVector(new boolean[] {false})))
                            .build());
            I64Vector dictionaryChildGroups = new I64Vector(2);
            state.assignGroups(new Vector[] {dictionaryChildren}, new Vector[] {null}, Mask.all(2), dictionaryChildGroups);
            assertThat(dictionaryChildGroups.values()).containsExactly(9, 9);

            MapVector rleChildren = new MapVector(2);
            System.arraycopy(new int[] {0, 2, 4}, 0, rleChildren.offsets(), 0, 3);
            rleChildren.setEntries(
                    Streams.ofValues(new RleVector(new int[] {4}, new I64Vector(new long[] {7}))),
                    Streams.builder()
                            .put(org.weakref.nitro.data.Stream.VALUES, new RleVector(new int[] {4}, utf8("same")))
                            .put(org.weakref.nitro.data.Stream.NULLS, new RleVector(new int[] {4}, new BooleanVector(new boolean[] {false})))
                            .build());
            I64Vector rleChildGroups = new I64Vector(2);
            state.assignGroups(new Vector[] {rleChildren}, new Vector[] {null}, Mask.all(2), rleChildGroups);
            assertThat(rleChildGroups.values()).containsExactly(10, 10);
            assertThat(state.groupCount()).isEqualTo(11);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testNullableBinaryGroupedOutputIgnoresNullPayloadMetadataWhenSizing()
    {
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("nullable-binary-grouped-output");
        Vector[] values = {utf8("ignored", "ok")};
        Vector[] nulls = {new BooleanVector(new boolean[] {true, false})};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        try {
            table.beginBatch(values, nulls);
            assertThat(table.assignGroup(values, nulls, 0, 0)).isZero();
            assertThat(table.assignGroup(values, nulls, 1, 1)).isEqualTo(1);
            table.endBatch();

            // Null flat fields do not initialize their payload slot. Poison the binary length to prove that exact
            // output sizing consults the null bit before interpreting unspecified payload metadata.
            int nullRecord = table.recordIndex(0);
            int lengthOffset = table.keyOffset(table.fixedOffset(nullRecord)) + layout.field(0).fixedOffset() + Integer.BYTES * 2;
            Arrays.fill(table.fixedChunk(nullRecord), lengthOffset, lengthOffset + Integer.BYTES, (byte) 0xFF);

            Streams grouped = table.groupedValues(0, Mask.all(2), null, allocator, context);
            BinaryVector groupedValues = (BinaryVector) grouped.values();
            BooleanVector groupedNulls = (BooleanVector) grouped.getOrNull(org.weakref.nitro.data.Stream.NULLS);
            assertThat(groupedValues.byteCapacity()).isEqualTo(2);
            assertThat(groupedValues.length(1)).isEqualTo(2);
            assertThat(new String(groupedValues.copyBytes(1), StandardCharsets.UTF_8)).isEqualTo("ok");
            assertThat(groupedNulls.values()).containsExactly(true, false);
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testBatchedGroupedValueCopyPreservesPhysicalKindsAndNulls()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("batched-grouped-value-copy");
        Vector[] values = {
                new I64Vector(new long[] {10, 20, 10, 30}),
                new BooleanVector(new boolean[] {true, false, true, true}),
                new F64Vector(new double[] {1.5, 2.5, 1.5, 3.5}),
                utf8("a", "b", "a", "unused")};
        Vector[] nulls = {
                null,
                new BooleanVector(new boolean[] {false, true, false, false}),
                null,
                new BooleanVector(new boolean[] {false, false, false, true})};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 4, true);
        try {
            table.beginBatch(values, nulls);
            assertThat(table.assignGroup(values, nulls, 0, 0)).isZero();
            assertThat(table.assignGroup(values, nulls, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(values, nulls, 2, 2)).isZero();
            assertThat(table.assignGroup(values, nulls, 3, 2)).isEqualTo(2);
            table.endBatch();

            int[] sourcePositions = {2, 0, 1};
            Streams longs = table.copyGroupedValuePositions(0, null, sourcePositions, 0, 3, 0, 3, allocator, context);
            assertThat(((I64Vector) longs.values()).values()).containsExactly(30, 10, 20);
            assertThat(((BooleanVector) longs.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values()).containsExactly(false, false, false);

            Streams booleans = table.copyGroupedValuePositions(1, null, sourcePositions, 0, 3, 0, 3, allocator, context);
            assertThat(((BooleanVector) booleans.values()).values()[0]).isTrue();
            assertThat(((BooleanVector) booleans.values()).values()[1]).isTrue();
            assertThat(((BooleanVector) booleans.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values()).containsExactly(false, false, true);

            Streams doubles = table.copyGroupedValuePositions(2, null, sourcePositions, 0, 3, 0, 3, allocator, context);
            assertThat(((F64Vector) doubles.values()).values()).containsExactly(3.5, 1.5, 2.5);
            assertThat(((BooleanVector) doubles.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values()).containsExactly(false, false, false);

            Streams binary = table.copyGroupedValuePositions(3, null, sourcePositions, 0, 3, 0, 3, allocator, context);
            assertThat(((BooleanVector) binary.getOrNull(org.weakref.nitro.data.Stream.NULLS)).values()).containsExactly(true, false, false);
            assertThat(OperatorVectorSupport.binaryEquals(binary.values(), 1, "a".getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(OperatorVectorSupport.binaryEquals(binary.values(), 2, "b".getBytes(StandardCharsets.UTF_8))).isTrue();
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testNullableDictionaryProbeMatchesIdOnlyRecordAfterValueIdOverflow()
    {
        String[] dictionaryValues = new String[DEFAULT_VALUE_ID_CEILING + 1];
        int[] dictionaryIds = new int[dictionaryValues.length];
        for (int index = 0; index < dictionaryValues.length; index++) {
            dictionaryValues[index] = "overflow-value-" + index;
            dictionaryIds[index] = index;
        }
        Vector[] initial = {DictionaryVector.wrap(dictionaryIds, dictionaryIds.length, utf8(dictionaryValues))};
        Vector[] initialNulls = {new BooleanVector(new boolean[dictionaryIds.length])};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                initial,
                true,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 4, true);
        try {
            table.beginBatch(initial, initialNulls);
            assertThat(table.assignGroup(initial, initialNulls, 0, 0)).isZero();
            table.endBatch();

            Vector[] repeated = {DictionaryVector.wrap(new int[] {0, 0}, 2, utf8(dictionaryValues[0]))};
            Vector[] mixedNulls = {new BooleanVector(new boolean[] {false, true})};
            table.beginBatch(repeated, mixedNulls);
            assertThat(table.assignGroup(repeated, mixedNulls, 0, 1)).isZero();
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveCompactLongRecordPreservesLaterFullWidthValues()
    {
        TypeBinding bigint = longBinding("testing:bigint", Optional.empty());
        long[] firstValues = new long[4096];
        int[] secondValues = new int[4096];
        firstValues[0] = 17;
        firstValues[1] = -23;
        firstValues[2] = 17;
        secondValues[0] = 1;
        secondValues[1] = 2;
        secondValues[2] = 1;
        int[] firstDictionaryIds = new int[4096];
        firstDictionaryIds[1] = 1;
        Vector[] first = {
                new I64Vector(firstValues),
                new I32Vector(secondValues),
                DictionaryVector.wrap(firstDictionaryIds, firstDictionaryIds.length, utf8("alpha", "beta"))};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                first,
                true,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                List.of(bigint, bigint));
        assertThat(layout.fixedRecordSize()).isEqualTo(1 + 2 * Integer.BYTES + 3 * Integer.BYTES);

        FlatGroupingTable table = new FlatGroupingTable(layout, 4, true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("adaptive-compact-long-record");
        try {
            table.beginBatch(first, null);
            assertThat(table.assignGroup(first, null, 0, 0)).isZero();
            assertThat(table.assignGroup(first, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(first, null, 2, 2)).isZero();
            table.endBatch();

            long widePositive = (1L << 50) + 31;
            long wideNegative = -(1L << 52) + 7;
            Vector[] later = {
                    new I64Vector(new long[] {widePositive, wideNegative, widePositive, 17}),
                    new I64Vector(new long[] {3, 4, 3, 1}),
                    DictionaryVector.wrap(new int[] {0, 1, 0, 0}, 4, utf8("alpha", "beta"))};
            table.beginBatch(later, null);
            assertThat(table.assignGroup(later, null, 0, 2)).isEqualTo(2);
            assertThat(table.assignGroup(later, null, 1, 3)).isEqualTo(3);
            assertThat(table.assignGroup(later, null, 2, 4)).isEqualTo(2);
            assertThat(table.assignGroup(later, null, 3, 4)).isZero();
            table.endBatch();

            I64Vector grouped = (I64Vector) table.groupedValues(0, Mask.all(4), null, allocator, allocationContext).values();
            assertThat(grouped.values()).containsExactly(17, -23, widePositive, wideNegative);
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testAdaptiveCompactLongRecordRejectsWideMultiDictionaryCube()
    {
        int[] ids = new int[4096];
        Vector[] values = {
                DictionaryVector.wrap(ids, ids.length, utf8("a")),
                DictionaryVector.wrap(ids, ids.length, utf8("b")),
                DictionaryVector.wrap(ids, ids.length, utf8("c")),
                DictionaryVector.wrap(ids, ids.length, utf8("d")),
                new I64Vector(4096)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                values,
                true,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy);
        assertThat(layout.fixedRecordSize()).isEqualTo(1 + 4 * 3 * Integer.BYTES + Long.BYTES);
    }

    @Test
    void testProviderOwnedIntegerStorageIsStableAcrossPhysicalVectorWidths()
    {
        TypeBinding bigint = longBinding("testing:bigint", Optional.empty());
        TypeBinding integer = longBinding("testing:integer", Optional.of(INTEGER_FLAT_KEY_STORAGE));
        Vector[] first = {
                new I64Vector(new long[] {10, 20, 10}),
                new I32Vector(new int[] {1, 2, 1})};
        FlatKeyLayout layout = BigintPairFlatKeyLayout.create(
                first,
                false,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                List.of(bigint, integer));
        assertThat(layout.fixedRecordSize()).isEqualTo(Long.BYTES + Integer.BYTES);

        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("provider-owned-integer-storage");
        try {
            table.beginBatch(first, null);
            assertThat(table.assignGroup(first, null, 0, 0)).isZero();
            assertThat(table.assignGroup(first, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(first, null, 2, 2)).isZero();
            table.endBatch();

            Vector[] later = {
                    new I64Vector(new long[] {20, 30}),
                    new I64Vector(new long[] {2, 3})};
            table.beginBatch(later, null);
            assertThat(table.assignGroup(later, null, 0, 2)).isEqualTo(1);
            assertThat(table.assignGroup(later, null, 1, 2)).isEqualTo(2);
            table.endBatch();

            I64Vector grouped = (I64Vector) table.groupedValues(1, Mask.all(3), null, allocator, allocationContext).values();
            assertThat(grouped.values()).containsExactly(1, 2, 3);
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testBigintPairSkipsProvenAbsentBatchNullsAndReadmitsLaterNulls()
    {
        Vector[] initial = {
                new I64Vector(new long[] {1, 2}),
                new I64Vector(new long[] {10, 20})};
        FlatKeyLayout layout = BigintPairFlatKeyLayout.create(
                initial,
                true,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                List.of());
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        try {
            Vector allFalse = new RleVector(new int[] {2}, new BooleanVector(new boolean[] {false}));
            Vector[] initialNulls = {allFalse, allFalse};
            table.beginBatch(initial, initialNulls);
            assertThat(table.assignGroup(initial, initialNulls, 0, 0)).isZero();
            assertThat(table.assignGroup(initial, initialNulls, 1, 1)).isEqualTo(1);
            table.endBatch();

            Vector[] nullable = {
                    new I64Vector(new long[] {1, 2, 1}),
                    new I64Vector(new long[] {10, 20, 10})};
            Vector[] nullableNulls = {
                    new BooleanVector(new boolean[] {false, false, true}),
                    new RleVector(new int[] {3}, new BooleanVector(new boolean[] {false}))};
            table.beginBatch(nullable, nullableNulls);
            assertThat(table.assignGroup(nullable, nullableNulls, 0, 2)).isZero();
            assertThat(table.assignGroup(nullable, nullableNulls, 1, 2)).isEqualTo(1);
            assertThat(table.assignGroup(nullable, nullableNulls, 2, 2)).isEqualTo(2);
            table.endBatch();

            Vector[] finalValues = {new I64Vector(new long[] {1}), new I64Vector(new long[] {10})};
            Vector finalAllFalse = new RleVector(new int[] {1}, new BooleanVector(new boolean[] {false}));
            Vector[] finalNulls = {finalAllFalse, finalAllFalse};
            table.beginBatch(finalValues, finalNulls);
            assertThat(table.assignGroup(finalValues, finalNulls, 0, 3)).isZero();
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testProviderOwnedIntegerStorageRejectsOutOfDomainValue()
    {
        TypeBinding bigint = longBinding("testing:bigint", Optional.empty());
        TypeBinding integer = longBinding("testing:integer", Optional.of(INTEGER_FLAT_KEY_STORAGE));
        Vector[] values = {
                new I64Vector(new long[] {10}),
                new I64Vector(new long[] {(long) Integer.MAX_VALUE + 1})};
        FlatKeyLayout layout = BigintPairFlatKeyLayout.create(
                values,
                false,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                List.of(bigint, integer));
        FlatGroupingTable table = new FlatGroupingTable(layout, 1, true);
        try {
            table.beginBatch(values, null);
            assertThatThrownBy(() -> table.assignGroup(values, null, 0, 0))
                    .isInstanceOf(ArithmeticException.class);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testNormalizedRecordWriteUsesProviderOwnedFieldWidth()
    {
        TypeBinding bigint = longBinding("testing:bigint", Optional.empty());
        TypeBinding integer = longBinding("testing:integer", Optional.of(INTEGER_FLAT_KEY_STORAGE));
        Vector[] values = {
                new I64Vector(new long[] {1}),
                new I64Vector(new long[] {2}),
                new I32Vector(new int[] {3})};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                values,
                true,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                List.of(bigint, bigint, integer));
        try {
            assertThat(layout.fixedRecordSize()).isEqualTo(1 + 2 * Long.BYTES + Integer.BYTES);
            assertThat(layout.supportsNormalizedRecordWrite()).isTrue();
            byte[] record = new byte[layout.fixedRecordSize()];
            layout.writeNormalizedRecord(record, 0, 2L | 3L << Integer.SIZE, 4L);
            layout.beginBatch(values, null);
            assertThat(layout.identicalRecordToInput(record, 0, null, values, null, 0, 0)).isTrue();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testProviderOwnedIntegerStoragePreservesHighCardinalitySignedPairs()
    {
        TypeBinding bigint = longBinding("testing:bigint", Optional.empty());
        TypeBinding integer = longBinding("testing:integer", Optional.of(INTEGER_FLAT_KEY_STORAGE));
        int batchSize = 4096;
        Vector[] initial = {new I64Vector(batchSize), new I32Vector(batchSize)};
        FlatKeyLayout layout = BigintPairFlatKeyLayout.create(
                initial,
                true,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                List.of(bigint, integer));
        FlatGroupingTable table = new FlatGroupingTable(layout, 16, true, true);
        Map<LongPair, Integer> expectedGroups = new HashMap<>();
        long[] expectedFirst = new long[20 * batchSize];
        long[] expectedSecond = new long[20 * batchSize];
        Random random = new Random(814735);
        int nextGroup = 0;
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("compact-high-cardinality-output");
        try {
            for (int batch = 0; batch < 20; batch++) {
                long[] first = new long[batchSize];
                int[] second = new int[batchSize];
                for (int position = 0; position < batchSize; position++) {
                    first[position] = random.nextLong();
                    second[position] = random.nextInt();
                    if (position >= 2 && position % 7 == 0) {
                        first[position] = first[position - 2];
                        second[position] = second[position - 2];
                    }
                }
                Vector[] values = {new I64Vector(first), new I32Vector(second)};
                Vector[] nulls = {new BooleanVector(batchSize), new BooleanVector(batchSize)};
                table.beginBatch(values, nulls);
                table.prepareBatchHashes(values, nulls, Mask.all(batchSize));
                for (int position = 0; position < batchSize; position++) {
                    LongPair key = new LongPair(first[position], second[position]);
                    Integer expected = expectedGroups.get(key);
                    long actual = table.assignGroup(values, nulls, position, nextGroup);
                    if (expected == null) {
                        expectedGroups.put(key, nextGroup);
                        expectedFirst[nextGroup] = key.first();
                        expectedSecond[nextGroup] = key.second();
                        assertThat(actual).isEqualTo(nextGroup);
                        nextGroup++;
                    }
                    else {
                        assertThat(actual).isEqualTo(expected.longValue());
                    }
                }
                table.endBatch();
            }
            I64Vector firstOutput = (I64Vector) table.groupedValues(0, Mask.all(nextGroup), null, allocator, allocationContext).values();
            I64Vector secondOutput = (I64Vector) table.groupedValues(1, Mask.all(nextGroup), null, allocator, allocationContext).values();
            assertThat(firstOutput.values()).startsWith(Arrays.copyOf(expectedFirst, nextGroup));
            assertThat(secondOutput.values()).startsWith(Arrays.copyOf(expectedSecond, nextGroup));
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testProviderOwnedIntegerStoragePreservesDictionaryBackedFourFieldGroups()
    {
        int dictionarySize = 28_853;
        int stringCount = 358;
        String[] strings = new String[stringCount];
        int[] integers = new int[dictionarySize];
        for (int index = 0; index < stringCount; index++) {
            strings[index] = "brand-" + index;
        }
        for (int index = 0; index < dictionarySize; index++) {
            integers[index] = index * 401 - 5_000_000;
        }

        TypeBinding integer = longBinding("testing:integer", Optional.of(INTEGER_FLAT_KEY_STORAGE));
        TypeBinding unspecified = org.weakref.nitro.core.type.Schema.unspecified(1).field(0).type();
        int batchSize = 4_096;
        Vector[] samples = {
                DictionaryVector.wrap(new int[batchSize], batchSize, utf8(strings)),
                DictionaryVector.wrap(new int[batchSize], batchSize, new I32Vector(integers)),
                new I32Vector(batchSize),
                new I32Vector(batchSize)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(
                samples,
                true,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy,
                List.of(unspecified, integer, integer, integer));
        assertThat(layout.fixedRecordSize()).isEqualTo(1 + 12 + 3 * Integer.BYTES);

        FlatGroupingTable table = new FlatGroupingTable(layout, 16, true);
        Map<FourFieldKey, Integer> expectedGroups = new HashMap<>();
        Random random = new Random(719913);
        int nextGroup = 0;
        try {
            for (int batch = 0; batch < 12; batch++) {
                int[] stringIds = new int[batchSize];
                int[] integerIds = new int[batchSize];
                int[] hours = new int[batchSize];
                int[] minutes = new int[batchSize];
                for (int position = 0; position < batchSize; position++) {
                    stringIds[position] = random.nextInt(stringCount);
                    integerIds[position] = random.nextInt(dictionarySize);
                    hours[position] = random.nextInt(24);
                    minutes[position] = random.nextInt(60);
                    if (position >= 3 && position % 11 == 0) {
                        stringIds[position] = stringIds[position - 3];
                        integerIds[position] = integerIds[position - 3];
                        hours[position] = hours[position - 3];
                        minutes[position] = minutes[position - 3];
                    }
                }
                Vector[] values = {
                        DictionaryVector.wrap(stringIds, batchSize, utf8(strings)),
                        DictionaryVector.wrap(integerIds, batchSize, new I32Vector(integers)),
                        new I32Vector(hours),
                        new I32Vector(minutes)};
                Vector[] nulls = {null, null, null, null};
                table.beginBatch(values, nulls);
                table.prepareBatchHashes(values, nulls, Mask.all(batchSize));
                for (int position = 0; position < batchSize; position++) {
                    FourFieldKey key = new FourFieldKey(
                            stringIds[position],
                            integers[integerIds[position]],
                            hours[position],
                            minutes[position]);
                    Integer expected = expectedGroups.get(key);
                    long actual = table.assignGroup(values, nulls, position, nextGroup);
                    if (expected == null) {
                        expectedGroups.put(key, nextGroup);
                        assertThat(actual).isEqualTo(nextGroup);
                        nextGroup++;
                    }
                    else {
                        assertThat(actual).isEqualTo(expected.longValue());
                    }
                }
                table.endBatch();
            }
            assertThat(table.recordCount()).isEqualTo(expectedGroups.size());
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testSharedDictionaryCacheTracksFixedWidthContentGeneration()
    {
        int size = 2_048;
        int[] ids = new int[size];
        I32Vector integerBase = new I32Vector(new int[] {1});
        BinaryVector binaryBase = utf8("brand");
        TypeBinding integer = longBinding("testing:integer", Optional.of(INTEGER_FLAT_KEY_STORAGE));
        TypeBinding unspecified = org.weakref.nitro.core.type.Schema.unspecified(1).field(0).type();
        GroupingState grouping = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(unspecified, integer),
                null,
                null);
        Vector[] nulls = {null, null};
        try {
            Vector[] first = {
                    DictionaryVector.wrap(ids, size, binaryBase),
                    DictionaryVector.wrap(ids, size, integerBase)};
            I64Vector firstGroups = new I64Vector(size);
            grouping.assignGroups(first, nulls, Mask.all(size), firstGroups);
            assertThat(firstGroups.values()).containsOnly(0);

            integerBase.clearForReuse();
            integerBase.values()[0] = 2;
            Vector[] second = {
                    DictionaryVector.wrap(ids, size, binaryBase),
                    DictionaryVector.wrap(ids, size, integerBase)};
            I64Vector secondGroups = new I64Vector(size);
            grouping.assignGroups(second, nulls, Mask.all(size), secondGroups);
            assertThat(secondGroups.values()).containsOnly(1);
        }
        finally {
            grouping.releaseBuffers();
        }
    }

    @Test
    void testSingleLongGroupingInitialCapacityUsesSampledCardinality()
    {
        int size = 100_000;
        long[] lowCardinality = new long[size];
        long[] highCardinality = new long[size];
        for (int position = 0; position < size; position++) {
            lowCardinality[position] = position % 12;
            highCardinality[position] = position;
        }

        GroupingState low = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        GroupingState high = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            low.initializeSchema(new Vector[] {new I64Vector(lowCardinality)}, new Vector[] {null});
            high.initializeSchema(new Vector[] {new I64Vector(highCardinality)}, new Vector[] {null});

            assertThat(low.longGroupIds.length).isLessThanOrEqualTo(128);
            assertThat(high.longGroupIds.length).isEqualTo(262_144);
        }
        finally {
            low.releaseBuffers();
            high.releaseBuffers();
        }
    }

    @Test
    void testHighCardinalitySingleLongGroupingDropsDuplicateSlotKeysWithoutRuns()
    {
        int size = groupingResources.longGroupingPolicy().idIndexedMinGroups();
        long[] keys = new long[size];
        for (int position = 0; position < size; position++) {
            // Unique, sparse keys presented without adjacent reuse: high cardinality alone should make the
            // duplicate slot-key array more expensive than exact equality through the dense canonical map.
            keys[position] = ((long) position << 32) | Integer.toUnsignedLong(position * 0x9E37_79B9);
        }

        GroupingState grouping = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            grouping.assignGroups(new I64Vector(keys), null, Mask.all(size), new I64Vector(size));

            int ordinarySlots = grouping.longGroupIds.length;
            assertThat(grouping.prepareSingleLongIdIndexedGrouping(false, size + 1L)).isTrue();
            assertThat(grouping.usesIdIndexedLongGrouping()).isTrue();
            assertThat(grouping.longGroupKeys).isEmpty();
            assertThat(grouping.longGroupIds).hasSize(
                    ordinarySlots * groupingResources.longGroupingPolicy().idIndexedUnclusteredActivationCapacityMultiplier());
        }
        finally {
            grouping.releaseBuffers();
        }
    }

    @Test
    void testSingleLongGroupingSamplesSelectedPhysicalPositions()
    {
        long[] values = new long[100];
        for (int position = 0; position < values.length; position++) {
            values[position] = position;
        }

        GroupingState grouping = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            grouping.initializeSchema(
                    new Vector[] {new I64Vector(values)},
                    new Vector[] {new BooleanVector(new boolean[10])},
                    Mask.all(10));

            assertThat(grouping.longGroupIds.length).isEqualTo(32);
        }
        finally {
            grouping.releaseBuffers();
        }
    }

    @Test
    void testGroupingRejectsLaterVectorOutsidePlanTimeTypeBinding()
    {
        TypeBinding binaryOnly = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:binary-only");
            }

            @Override
            public Class<?> carrierType()
            {
                return byte[].class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(BinaryVector.class);
            }
        };
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("testGroupingTypeBinding");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy,
                List.of(binaryOnly),
                allocator,
                allocationContext);
        state.assignGroups(
                utf8("supported"),
                null,
                Mask.all(1),
                new I64Vector(1));

        assertThatThrownBy(() -> state.assignGroups(
                new I64Vector(new long[] {1}),
                null,
                Mask.all(1),
                new I64Vector(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("testing:binary-only");
        state.releaseBuffers();
        allocator.release(allocationContext);
    }

    @Test
    void testGeneratedDictionaryHashNullFreePairRequiresEnoughRows()
    {
        int smallSize = 128;
        int[] smallIds = new int[smallSize];
        for (int position = 0; position < smallSize; position++) {
            smallIds[position] = position % 3;
        }
        Vector[] small = {
                DictionaryVector.wrap(smallIds, smallSize, new I64Vector(new long[] {10, 20, 30})),
                DictionaryVector.wrap(smallIds, smallSize, utf8("a", "b", "c"))};
        FlatKeyLayout smallLayout = FlatKeyLayout.tryCreate(small, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            smallLayout.beginBatch(small, null);
            assertThat(smallLayout.prepareGeneratedDictionaryBatchHashes(smallSize, new long[smallSize])).isFalse();
            smallLayout.endBatch();
        }
        finally {
            smallLayout.releaseBuffers();
        }

        int largeSize = 2048;
        int[] largeIds = new int[largeSize];
        for (int position = 0; position < largeSize; position++) {
            largeIds[position] = position % 3;
        }
        Vector[] large = {
                DictionaryVector.wrap(largeIds, largeSize, new I64Vector(new long[] {10, 20, 30})),
                DictionaryVector.wrap(largeIds, largeSize, utf8("a", "b", "c"))};
        FlatKeyLayout largeLayout = FlatKeyLayout.tryCreate(large, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            largeLayout.beginBatch(large, null);
            assertThat(largeLayout.prepareGeneratedDictionaryBatchHashes(largeSize, new long[largeSize])).isTrue();
            largeLayout.endBatch();
        }
        finally {
            largeLayout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedHybridHashRequiresTwoAccessorFields()
    {
        int size = 128;
        int[] ids = new int[size];
        long[] largeLongDictionary = new long[size + 1];
        for (int position = 0; position < size; position++) {
            ids[position] = position % 3;
            largeLongDictionary[position] = position;
        }
        Vector[] values = {
                DictionaryVector.wrap(ids, size, new I64Vector(largeLongDictionary)),
                DictionaryVector.wrap(ids, size, utf8("a", "b", "c")),
                DictionaryVector.wrap(ids, size, new I64Vector(new long[] {10, 20, 30})),
                DictionaryVector.wrap(ids, size, utf8("d", "e", "f")),
                DictionaryVector.wrap(ids, size, utf8("g", "h", "i"))};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, false, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, null);
            assertThat(layout.prepareGeneratedDictionaryBatchHashes(size, new long[size])).isFalse();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedHybridHashRetainsFixedWidthAdmissionThreshold()
    {
        int size = 128;
        Vector[] values = {
                new I64Vector(new long[size]),
                new I64Vector(new long[size]),
                new I64Vector(new long[size]),
                new I64Vector(new long[size])};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, false, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, null);
            assertThat(layout.prepareGeneratedDictionaryBatchHashes(size, new long[size])).isFalse();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedHybridHashDefersReusableFlatBinaryDomain()
    {
        int size = 128;
        String[] repeated = new String[size];
        long[] first = new long[size];
        long[] second = new long[size];
        for (int position = 0; position < size; position++) {
            repeated[position] = "value-" + position % 8;
            first[position] = position;
            second[position] = position * 31L;
        }
        Vector[] lowCardinality = {
                utf8(repeated),
                new I64Vector(first),
                new I64Vector(second)};
        FlatKeyLayout lowCardinalityLayout = FlatKeyLayout.tryCreate(
                lowCardinality,
                false,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy);
        try {
            lowCardinalityLayout.beginBatch(lowCardinality, null);
            assertThat(lowCardinalityLayout.prepareGeneratedDictionaryBatchHashes(size, new long[size])).isFalse();
            lowCardinalityLayout.endBatch();
        }
        finally {
            lowCardinalityLayout.releaseBuffers();
        }

        String[] unique = new String[size];
        for (int position = 0; position < size; position++) {
            unique[position] = "unique-value-" + position;
        }
        Vector[] highCardinality = {
                utf8(unique),
                new I64Vector(first),
                new I64Vector(second)};
        FlatKeyLayout highCardinalityLayout = FlatKeyLayout.tryCreate(
                highCardinality,
                false,
                arrayPool,
                codeGeneration,
                flatKeyTablePolicy);
        try {
            highCardinalityLayout.beginBatch(highCardinality, null);
            assertThat(highCardinalityLayout.prepareGeneratedDictionaryBatchHashes(size, new long[size])).isTrue();
            highCardinalityLayout.endBatch();
        }
        finally {
            highCardinalityLayout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedDictionaryHashBatchMatchesLogicalHashAcrossNullShapes()
    {
        int size = 128;
        int[] ids = new int[size];
        long[] largeLongDictionary = new long[size + 1];
        boolean[] allNullValues = new boolean[size];
        boolean[] mixedNullValues = new boolean[size];
        for (int position = 0; position < size; position++) {
            ids[position] = position % 3;
            largeLongDictionary[position] = position * 10L;
            allNullValues[position] = true;
            mixedNullValues[position] = (position & 7) == 0;
        }
        Vector[] values = {
                // These bases are larger than the live batch, so the layout must retain its cost guard and hash the
                // fields through resolved long/binary accessors rather than eagerly pre-hashing every dictionary
                // entry. The remaining dictionary lanes stay direct in the same generated kernel.
                DictionaryVector.wrap(ids, size, new I64Vector(largeLongDictionary)),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("oversized-", size + 1))),
                DictionaryVector.wrap(ids, size, new I64Vector(largeLongDictionary.clone())),
                DictionaryVector.wrap(ids, size, utf8("d", "e", "f")),
                DictionaryVector.wrap(ids, size, utf8("g", "h", "i")),
                // SQL joins can change one key lane from dictionary to multi-run RLE between batches. The generated
                // hybrid must resolve that physical representation once rather than rejecting the complete key.
                new RleVector(new int[] {size / 2, size / 2}, utf8("rle-a", "rle-b"))};
        Vector[] nulls = {
                null,
                new BooleanVector(allNullValues),
                new BooleanVector(mixedNullValues),
                null,
                null,
                null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, nulls);
            long[] expected = new long[size];
            for (int position = 0; position < size; position++) {
                expected[position] = layout.hash(values, nulls, position);
            }
            long[] actual = new long[size];
            assertThat(layout.prepareGeneratedDictionaryBatchHashes(size, actual)).isTrue();
            assertThat(actual).containsExactly(expected);
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedDictionaryHashBatchMatchesNineFieldLogicalHash()
    {
        int size = 128;
        int[] ids = new int[size];
        for (int position = 0; position < size; position++) {
            ids[position] = position % 3;
        }
        long[] constantGroupId = new long[size];
        java.util.Arrays.fill(constantGroupId, 7);
        Vector[] values = {
                DictionaryVector.wrap(ids, size, utf8("a", "b", "c")),
                DictionaryVector.wrap(ids, size, new I64Vector(new long[] {10, 20, 30})),
                DictionaryVector.wrap(ids, size, utf8("d", "e", "f")),
                DictionaryVector.wrap(ids, size, new I64Vector(new long[] {40, 50, 60})),
                DictionaryVector.wrap(ids, size, utf8("g", "h", "i")),
                DictionaryVector.wrap(ids, size, new I64Vector(new long[] {70, 80, 90})),
                DictionaryVector.wrap(ids, size, utf8("j", "k", "l")),
                DictionaryVector.wrap(ids, size, new I64Vector(new long[] {100, 110, 120})),
                new I64Vector(constantGroupId)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, false, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, null);
            long[] expected = new long[size];
            for (int position = 0; position < size; position++) {
                expected[position] = layout.hash(values, null, position);
            }
            long[] actual = new long[size];
            assertThat(layout.prepareGeneratedDictionaryBatchHashes(size, actual)).isTrue();
            assertThat(actual).containsExactly(expected);
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testPhysicalLongBindingResolvesDictionaryRegionCompositions()
    {
        int[] ids = {2, 0, 1, 2};
        Vector[] values = {
                DictionaryVector.wrap(
                        ids,
                        ids.length,
                        new RegionVector(new I32Vector(new int[] {-1, 10, 20, 30, -1}), 1, 3)),
                new RegionVector(
                        DictionaryVector.wrap(
                                new int[] {0, 2, 1, 0, 2, 1},
                                new I64Vector(new long[] {100, 200, 300})),
                        1,
                        ids.length),
                DictionaryVector.wrap(
                        ids,
                        ids.length,
                        DictionaryVector.wrap(
                                new int[] {1, 2, 0},
                                new RegionVector(new I64Vector(new long[] {-1, 1_000, 2_000, 3_000, -1}), 1, 3)))};
        long[][] expected = {
                {30, 10, 20, 30},
                {300, 200, 100, 300},
                {1_000, 2_000, 3_000, 1_000}};

        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, false, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, null);
            for (int field = 0; field < values.length; field++) {
                for (int position = 0; position < ids.length; position++) {
                    assertThat(layout.fieldHash(field, field, values[field], position))
                            .isEqualTo(Long.hashCode(expected[field][position]));
                }
            }
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedDictionaryHashProbeBatchMatchesDecoupledDriver()
    {
        int size = 128;
        int[] ids = new int[size];
        long[] largeLongDictionary = new long[size + 1];
        boolean[] mixedFirstNulls = new boolean[size];
        boolean[] mixedSecondNulls = new boolean[size];
        for (int position = 0; position < size; position++) {
            ids[position] = position % 17;
            largeLongDictionary[position] = position * 10L;
            mixedFirstNulls[position] = position % 13 == 0;
            mixedSecondNulls[position] = position % 11 == 0;
        }
        Vector[] values = {
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("first-", 17))),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("second-", 17))),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("third-", 17))),
                DictionaryVector.wrap(ids, size, new I64Vector(largeLongDictionary)),
                DictionaryVector.wrap(ids, size, new I64Vector(largeLongDictionary.clone()))};
        Vector[] nulls = {
                new BooleanVector(mixedFirstNulls),
                new BooleanVector(mixedSecondNulls),
                null,
                null,
                null};

        FlatGroupingTable fused = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        FlatGroupingTable decoupled = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        try {
            I64Vector fusedGroups = new I64Vector(size);
            fused.beginBatch(values, nulls);
            long fusedNextGroup = fused.assignGeneratedDictionaryBatch(
                    values, nulls, Mask.all(size), fusedGroups, 0);
            fused.endBatch();
            assertThat(fusedNextGroup).isGreaterThan(0);

            I64Vector decoupledGroups = new I64Vector(size);
            decoupled.beginBatch(values, nulls);
            decoupled.prepareBatchHashes(values, nulls, Mask.all(size));
            long decoupledNextGroup = 0;
            for (int position = 0; position < size; position++) {
                long group = decoupled.assignGroup(values, nulls, position, decoupledNextGroup);
                if (group == decoupledNextGroup) {
                    decoupledNextGroup++;
                }
                decoupledGroups.values()[position] = group;
            }
            decoupled.endBatch();

            assertThat(fusedNextGroup).isEqualTo(decoupledNextGroup);
            assertThat(fusedGroups.values()).containsExactly(decoupledGroups.values());
            assertThat(fused.recordCount()).isEqualTo(decoupled.recordCount());
        }
        finally {
            fused.releaseBuffers();
            decoupled.releaseBuffers();
        }
    }

    @Test
    void testGeneratedDictionaryRecordEqualityMatchesIdsLongsNullsAndFallbackRecords()
    {
        int distinct = 64;
        int size = distinct * 2;
        int[] ids = new int[size];
        String[] firstBase = new String[distinct];
        String[] secondBase = new String[distinct];
        String[] thirdBase = new String[distinct];
        String[] firstFlat = new String[size];
        String[] secondFlat = new String[size];
        String[] thirdFlat = new String[size];
        long[] years = new long[size];
        long[] months = new long[size];
        boolean[] firstNulls = new boolean[size];
        boolean[] secondNulls = new boolean[size];
        for (int value = 0; value < distinct; value++) {
            firstBase[value] = "first-" + value;
            secondBase[value] = "second-" + value;
            thirdBase[value] = "third-" + value;
        }
        for (int position = 0; position < size; position++) {
            int value = position % distinct;
            ids[position] = value;
            firstFlat[position] = firstBase[value];
            secondFlat[position] = secondBase[value];
            thirdFlat[position] = thirdBase[value];
            years[position] = 1998 + value % 3;
            months[position] = 1 + value % 12;
            firstNulls[position] = value % 7 == 0;
            secondNulls[position] = value % 11 == 0;
        }
        Vector[] dictionaryValues = {
                DictionaryVector.wrap(ids, size, utf8(firstBase)),
                DictionaryVector.wrap(ids, size, utf8(secondBase)),
                DictionaryVector.wrap(ids, size, utf8(thirdBase)),
                new I64Vector(years),
                new I64Vector(months)};
        Vector[] flatValues = {
                utf8(firstFlat),
                utf8(secondFlat),
                utf8(thirdFlat),
                new I64Vector(years.clone()),
                new I64Vector(months.clone())};
        Vector[] nulls = {
                new BooleanVector(firstNulls),
                new BooleanVector(secondNulls),
                null,
                null,
                null};

        FlatKeyLayout idLayout = FlatKeyLayout.tryCreate(dictionaryValues, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable idRecords = new FlatGroupingTable(idLayout, 16, true);
        try {
            idRecords.beginBatch(dictionaryValues, nulls);
            assertThat(idLayout.usesGeneratedDictionaryRecordEquality()).isTrue();
            long nextGroup = 0;
            for (int position = 0; position < size; position++) {
                if (position == distinct) {
                    byte[] fixedChunk = idRecords.fixedChunk(0);
                    int fixedOffset = idRecords.keyOffset(idRecords.fixedOffset(0));
                    assertThat(idLayout.generatedDictionaryRecordEquality(
                            fixedChunk,
                            fixedOffset,
                            idRecords.variableWidthArena(),
                            dictionaryValues,
                            position,
                            0)).isEqualTo(DictionaryRecordEqualityKernel.IDENTICAL);
                }
                long group = idRecords.assignGroup(dictionaryValues, nulls, position, nextGroup);
                assertThat(group).isEqualTo(position % distinct);
                nextGroup += group == nextGroup ? 1 : 0;
            }
            idRecords.endBatch();
            assertThat(idRecords.recordCount()).isEqualTo(distinct);

            Vector[] newDictionaryIdentity = {
                    DictionaryVector.wrap(ids.clone(), size, utf8(firstBase)),
                    DictionaryVector.wrap(ids.clone(), size, utf8(secondBase)),
                    DictionaryVector.wrap(ids.clone(), size, utf8(thirdBase)),
                    new I64Vector(years.clone()),
                    new I64Vector(months.clone())};
            idRecords.beginBatch(newDictionaryIdentity, nulls);
            assertThat(idLayout.usesGeneratedDictionaryRecordEquality()).isTrue();
            for (int position = 0; position < size; position++) {
                assertThat(idRecords.assignGroup(newDictionaryIdentity, nulls, position, distinct)).isEqualTo(position % distinct);
            }
            idRecords.endBatch();
            assertThat(idRecords.recordCount()).isEqualTo(distinct);
        }
        finally {
            idRecords.releaseBuffers();
        }

        FlatKeyLayout fallbackLayout = FlatKeyLayout.tryCreate(dictionaryValues, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable fallbackRecords = new FlatGroupingTable(fallbackLayout, 16, true);
        try {
            fallbackRecords.beginBatch(flatValues, nulls);
            assertThat(fallbackLayout.usesGeneratedDictionaryRecordEquality()).isTrue();
            long nextGroup = 0;
            for (int position = 0; position < distinct; position++) {
                long group = fallbackRecords.assignGroup(flatValues, nulls, position, nextGroup);
                assertThat(group).isEqualTo(position);
                nextGroup++;
            }
            byte[] fixedChunk = fallbackRecords.fixedChunk(0);
            int fixedOffset = fallbackRecords.keyOffset(fallbackRecords.fixedOffset(0));
            assertThat(fallbackLayout.generatedDictionaryRecordEquality(
                    fixedChunk,
                    fixedOffset,
                    fallbackRecords.variableWidthArena(),
                    flatValues,
                    distinct,
                    0)).isEqualTo(DictionaryRecordEqualityKernel.IDENTICAL);
            fallbackRecords.endBatch();

            fallbackRecords.beginBatch(dictionaryValues, nulls);
            assertThat(fallbackLayout.usesGeneratedDictionaryRecordEquality()).isTrue();
            for (int position = 0; position < size; position++) {
                assertThat(fallbackRecords.assignGroup(dictionaryValues, nulls, position, distinct)).isEqualTo(position % distinct);
            }
            fallbackRecords.endBatch();
            assertThat(fallbackRecords.recordCount()).isEqualTo(distinct);
        }
        finally {
            fallbackRecords.releaseBuffers();
        }

        FlatKeyLayout nonNullableLayout = FlatKeyLayout.tryCreate(dictionaryValues, false, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable nonNullableRecords = new FlatGroupingTable(nonNullableLayout, 16, true);
        try {
            Vector[] noNulls = new Vector[dictionaryValues.length];
            nonNullableRecords.beginBatch(dictionaryValues, noNulls);
            assertThat(nonNullableLayout.usesGeneratedDictionaryRecordEquality()).isTrue();
            long nextGroup = 0;
            for (int position = 0; position < size; position++) {
                long group = nonNullableRecords.assignGroup(dictionaryValues, noNulls, position, nextGroup);
                assertThat(group).isEqualTo(position % distinct);
                nextGroup += group == nextGroup ? 1 : 0;
            }
            nonNullableRecords.endBatch();
            assertThat(nonNullableRecords.recordCount()).isEqualTo(distinct);
        }
        finally {
            nonNullableRecords.releaseBuffers();
        }
    }

    @Test
    void testGeneratedRecordEqualitySupportsNarrowCompositeKeys()
    {
        Vector[] values = {
                new I64Vector(new long[] {7, 8, 7}),
                utf8("alpha", "beta", "alpha"),
                new I64Vector(new long[] {11, 12, 11})};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        try {
            table.beginBatch(values, nulls);
            assertThat(layout.usesGeneratedDictionaryRecordEquality()).isTrue();
            assertThat(table.assignGroup(values, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(values, nulls, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(values, nulls, 2, 2)).isEqualTo(0);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testGeneratedRecordEqualityRetainsFixedWidthAdmissionThreshold()
    {
        Vector[] values = {
                new I64Vector(new long[] {1}),
                new I64Vector(new long[] {2}),
                new I64Vector(new long[] {3}),
                new I64Vector(new long[] {4})};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 1, true);
        try {
            table.beginBatch(values, null);
            assertThat(layout.usesGeneratedDictionaryRecordEquality()).isFalse();
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testWideSparseCompositeCachePreservesOffsetsFallbackAndGrowth()
    {
        int uniqueSize = 96;
        int size = uniqueSize * 2;
        int[] smallIds = new int[size];
        int[] largeIds = new int[size];
        int[] mediumIds = new int[size];
        int[] constantIds = new int[size];
        long[] years = new long[size];
        long[] months = new long[size];
        for (int position = 0; position < size; position++) {
            int logicalPosition = position % uniqueSize;
            smallIds[position] = logicalPosition % 15;
            largeIds[position] = logicalPosition % 1023;
            mediumIds[position] = (logicalPosition / 3) % 15;
            years[position] = 1998 + logicalPosition % 3;
            months[position] = 1 + logicalPosition % 12;
        }
        Vector[] first = {
                new I64Vector(years),
                new I64Vector(months),
                DictionaryVector.wrap(smallIds, size, utf8(compactTestStrings("small-", 15))),
                DictionaryVector.wrap(largeIds, size, utf8(compactTestStrings("large-", 1023))),
                DictionaryVector.wrap(mediumIds, size, utf8(compactTestStrings("medium-", 15))),
                DictionaryVector.wrap(constantIds, size, utf8("constant"))};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(first, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        try {
            table.beginBatch(first, null);
            table.prepareBatchHashes(first, null, Mask.all(size));
            long[] firstGroups = new long[size];
            for (int position = 0; position < size; position++) {
                firstGroups[position] = table.assignGroup(first, null, position, position);
            }
            table.endBatch();
            assertThat(table.recordCount()).isEqualTo(uniqueSize);
            assertThat(table.sparseCompositeSize()).isEqualTo(uniqueSize);

            // New vector and dictionary identities with the same logical values must reproduce the stable
            // composite ids. The sparse cache has already grown through several capacities at this point.
            Vector[] repeated = {
                    new I64Vector(years.clone()),
                    new I64Vector(months.clone()),
                    DictionaryVector.wrap(smallIds.clone(), size, utf8(compactTestStrings("small-", 15))),
                    DictionaryVector.wrap(largeIds.clone(), size, utf8(compactTestStrings("large-", 1023))),
                    DictionaryVector.wrap(mediumIds.clone(), size, utf8(compactTestStrings("medium-", 15))),
                    DictionaryVector.wrap(constantIds.clone(), size, utf8("constant"))};
            table.beginBatch(repeated, null);
            table.prepareBatchHashes(repeated, null, Mask.all(size));
            for (int position = 0; position < size; position++) {
                assertThat(table.assignGroup(repeated, null, position, uniqueSize)).isEqualTo(firstGroups[position]);
            }
            table.endBatch();

            // A later numeric value outside the first batch's bounded offset declines the cache and falls through
            // to exact record hashing. Repeating it still finds the authoritative record rather than aliasing an
            // in-domain composite.
            Vector[] outside = {
                    new I64Vector(new long[] {2050, 2050}),
                    new I64Vector(new long[] {1, 1}),
                    DictionaryVector.wrap(new int[] {0, 0}, 2, utf8(compactTestStrings("small-", 15))),
                    DictionaryVector.wrap(new int[] {0, 0}, 2, utf8(compactTestStrings("large-", 1023))),
                    DictionaryVector.wrap(new int[] {0, 0}, 2, utf8(compactTestStrings("medium-", 15))),
                    DictionaryVector.wrap(new int[] {0, 0}, 2, utf8("constant"))};
            table.beginBatch(outside, null);
            table.prepareBatchHashes(outside, null, Mask.all(2));
            assertThat(table.assignGroup(outside, null, 0, uniqueSize)).isEqualTo(uniqueSize);
            assertThat(table.assignGroup(outside, null, 1, uniqueSize + 1L)).isEqualTo(uniqueSize);
            table.endBatch();
            assertThat(table.recordCount()).isEqualTo(uniqueSize + 1);
            assertThat(table.sparseCompositeSize()).isEqualTo(uniqueSize);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testEncodedDictionaryDomainBatchIsArityGeneralNullAwareAndLogicallyExact()
    {
        int[] firstIds = {0, 1, 2, 0, 1, 2, 0, 1};
        int[] secondIds = {0, 0, 0, 1, 1, 1, 0, 1};
        int[] thirdIds = {0, 0, 0, 1, 1, 1, 0, 1};
        Vector[] values = {
                DictionaryVector.wrap(firstIds, firstIds.length, utf8("x", "x", "y")),
                DictionaryVector.wrap(secondIds, secondIds.length, new I64Vector(new long[] {10, 20})),
                DictionaryVector.wrap(thirdIds, thirdIds.length, utf8("p", "q"))};
        Vector[] nulls = {
                new BooleanVector(new boolean[] {false, false, false, false, false, false, false, true}),
                null,
                null};
        Mask mask = Mask.sparse(new int[] {0, 1, 2, 3, 5, 7}, firstIds.length);
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                16,
                true);
        try {
            I64Vector groups = new I64Vector(firstIds.length);
            table.beginBatch(values, nulls, mask);
            assertThat(table.assignEncodedDictionaryDomainBatch(values, nulls, mask, groups, 0)).isEqualTo(5);
            assertThat(table.encodedDictionaryDomainBatchActive()).isTrue();
            table.endBatch();

            assertThat(groups.values()[0]).isZero();
            // Distinct physical dictionary ids containing equal bytes must still share one logical group.
            assertThat(groups.values()[1]).isZero();
            assertThat(groups.values()[2]).isEqualTo(1);
            assertThat(groups.values()[3]).isEqualTo(2);
            assertThat(groups.values()[5]).isEqualTo(3);
            assertThat(groups.values()[7]).isEqualTo(4);
            assertThat(table.recordCount()).isEqualTo(5);

            I64Vector denseGroups = new I64Vector(firstIds.length);
            table.beginBatch(values, nulls, Mask.all(firstIds.length));
            assertThat(table.assignEncodedDictionaryDomainBatch(values, nulls, Mask.all(firstIds.length), denseGroups, 5))
                    .isEqualTo(5);
            table.endBatch();

            assertThat(denseGroups.values()).containsExactly(0, 0, 1, 2, 2, 3, 0, 4);
            assertThat(table.recordCount()).isEqualTo(5);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testSparseCompositeCacheAdmitsSparseDirectRollupDomain()
    {
        int size = 128;
        int[] ids = new int[size];
        boolean[] nulls = new boolean[size];
        Arrays.fill(nulls, true);
        Vector[] values = {
                new I64Vector(new long[size]),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("category-", 1025))),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("class-", 1025)))};
        Vector[] nullVectors = {
                new BooleanVector(new boolean[size]),
                new BooleanVector(nulls),
                new BooleanVector(nulls.clone())};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                16,
                true);
        try {
            table.beginBatch(values, nullVectors);
            table.prepareBatchHashes(values, nullVectors, Mask.all(size));
            for (int position = 0; position < size; position++) {
                assertThat(table.assignGroup(values, nullVectors, position, position)).isZero();
            }
            table.endBatch();

            assertThat(table.recordCount()).isOne();
            assertThat(table.sparseCompositeSize()).isOne();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testWideSparseCompositeCacheRejectsDistinctSample()
    {
        int size = 128;
        int[] distinctIds = new int[size];
        int[] constantIds = new int[size];
        long[] years = new long[size];
        long[] months = new long[size];
        for (int position = 0; position < size; position++) {
            distinctIds[position] = position;
            years[position] = 2000 + position;
            months[position] = 1;
        }
        Vector[] values = {
                new I64Vector(years),
                new I64Vector(months),
                DictionaryVector.wrap(distinctIds, size, utf8(compactTestStrings("distinct-", size))),
                DictionaryVector.wrap(constantIds, size, utf8("first")),
                DictionaryVector.wrap(constantIds, size, utf8("second")),
                DictionaryVector.wrap(constantIds, size, utf8("third"))};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        try {
            table.beginBatch(values, null);
            table.prepareBatchHashes(values, null, Mask.all(size));
            for (int position = 0; position < size; position++) {
                assertThat(table.assignGroup(values, null, position, position)).isEqualTo(position);
            }
            table.endBatch();

            assertThat(table.recordCount()).isEqualTo(size);
            assertThat(table.sparseCompositeSize()).isZero();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testWideSparseCompositeCacheRejectsLocallyConstantSample()
    {
        int size = 128;
        int cardinality = 16;
        int[] ids = new int[size];
        long[] years = new long[size];
        long[] months = new long[size];
        for (int position = 0; position < size; position++) {
            int value = position % cardinality;
            ids[position] = value;
            years[position] = 2000 + value;
            months[position] = 1 + value % 12;
        }
        Vector[] values = {
                new I64Vector(years),
                new I64Vector(months),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("first-", cardinality))),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("second-", cardinality))),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("third-", cardinality))),
                DictionaryVector.wrap(ids, size, utf8(compactTestStrings("fourth-", cardinality)))};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        try {
            table.beginBatch(values, null);
            table.prepareBatchHashes(values, null, Mask.all(size));
            for (int position = 0; position < size; position++) {
                assertThat(table.assignGroup(values, null, position, position)).isEqualTo(position % cardinality);
            }
            table.endBatch();

            assertThat(table.recordCount()).isEqualTo(cardinality);
            assertThat(table.sparseCompositeSize()).isZero();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testNullFreeSingleBinaryOverwriteClearsReusedRecordNullFlag()
    {
        Vector[] values = {utf8("alpha", "beta")};
        Vector[] nulls = {new BooleanVector(new boolean[] {false, false})};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 2, true);
        try {
            // A process-wide pooled fixed-record chunk may contain a null flag from its preceding owner. The
            // null-free single-binary writer must establish the complete new record rather than depend on the
            // fresh-array zero default.
            int keyOffset = table.keyOffset(table.fixedOffset(0));
            table.fixedChunk(0)[keyOffset] = 1;

            table.beginBatch(values, nulls);
            assertThat(table.assignGroup(values, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.fieldNull(0, 0)).isFalse();
            assertThat(table.findGroup(values, nulls, 0)).isEqualTo(0);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testPreparedNullFreeSingleBinaryBatchKeepsEmptyAndNullDistinct()
    {
        Vector[] values = {utf8("", "alpha", "")};
        Vector[] nulls = {new BooleanVector(new boolean[] {false, false, false})};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                4,
                true);
        try {
            I64Vector groups = new I64Vector(3);
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, Mask.all(3));
            assertThat(table.assignPreparedPhysicalBatch(values, nulls, Mask.all(3), groups, 0)).isEqualTo(2);
            assertThat(groups.values()).containsExactly(0, 1, 0);
            table.endBatch();

            Vector[] nullableValues = {utf8("", "beta")};
            Vector[] nullableNulls = {new BooleanVector(new boolean[] {true, false})};
            table.beginBatch(nullableValues, nullableNulls);
            table.prepareBatchHashes(nullableValues, nullableNulls, Mask.all(2));
            assertThat(table.assignPreparedPhysicalBatch(
                    nullableValues, nullableNulls, Mask.all(2), new I64Vector(2), 2)).isEqualTo(-1);
            assertThat(table.assignGroup(nullableValues, nullableNulls, 0, 2)).isEqualTo(2);
            assertThat(table.assignGroup(nullableValues, nullableNulls, 1, 3)).isEqualTo(3);
            table.endBatch();

            Vector[] empty = {utf8("")};
            Vector[] emptyNulls = {new BooleanVector(new boolean[] {false})};
            I64Vector emptyGroup = new I64Vector(1);
            table.beginBatch(empty, emptyNulls);
            table.prepareBatchHashes(empty, emptyNulls, Mask.all(1));
            assertThat(table.assignPreparedPhysicalBatch(
                    empty, emptyNulls, Mask.all(1), emptyGroup, 4)).isEqualTo(4);
            assertThat(emptyGroup.values()).containsExactly(0);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testPreparedNullFreeSingleBinaryBatchPreservesHashDomainAcrossNullShapes()
    {
        // This value's binary hash is Integer.MAX_VALUE. Adding the grouping seed must happen in the table's
        // authoritative signed 32-bit domain: widening the addition changes the probe sequence and makes a later
        // null-free batch insert a duplicate group for a record created by the nullable path.
        Vector[] nullableValues = {utf8("overflow-152905119", "unused")};
        Vector[] nullableNulls = {new BooleanVector(new boolean[] {false, true})};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(nullableValues, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                2,
                true);
        try {
            table.beginBatch(nullableValues, nullableNulls);
            assertThat(table.assignGroup(nullableValues, nullableNulls, 0, 0)).isEqualTo(0);
            table.endBatch();

            Vector[] nullFreeValues = {utf8("overflow-152905119")};
            Vector[] nullFreeNulls = {new BooleanVector(new boolean[] {false})};
            I64Vector groups = new I64Vector(1);
            table.beginBatch(nullFreeValues, nullFreeNulls);
            table.prepareBatchHashes(nullFreeValues, nullFreeNulls, Mask.all(1));
            assertThat(table.assignPreparedPhysicalBatch(
                    nullFreeValues, nullFreeNulls, Mask.all(1), groups, 1)).isEqualTo(1);
            assertThat(groups.values()).containsExactly(0);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testPreparedNullFreeSingleBinaryIdentityBatchPrefetchesAcrossDuplicatesAndGrowth()
    {
        int positions = 512;
        String[] strings = new String[positions];
        long[] expectedGroups = new long[positions];
        Map<String, Long> groupIds = new HashMap<>();
        for (int position = 0; position < positions; position++) {
            // Repeat every third row after the first tile while retaining enough distinct keys to force growth.
            int key = position >= 96 && position % 3 == 0 ? position - 96 : position;
            strings[position] = "variable-width-key-" + key;
            expectedGroups[position] = groupIds.computeIfAbsent(strings[position], ignored -> (long) groupIds.size());
        }
        Vector[] values = {utf8(strings)};
        Vector[] nulls = {new BooleanVector(positions)};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                2,
                true);
        try {
            I64Vector groups = new I64Vector(positions);
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, Mask.all(positions));
            long nextGroupId = table.assignPreparedPhysicalBatch(values, nulls, Mask.all(positions), groups, 0);
            table.endBatch();

            assertThat(nextGroupId).isEqualTo(groupIds.size());
            assertThat(groups.values()).containsExactly(expectedGroups);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testHashRecordWidthFollowsImmutableKeyArity()
    {
        Vector[] single = {utf8("alpha", "beta")};
        FlatGroupingTable singleTable = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(single, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                2,
                true);
        try {
            assertThat(singleTable.usesIntHashRecords()).isTrue();
            singleTable.beginBatch(single, null);
            assertThat(singleTable.assignGroup(single, null, 0, 0)).isEqualTo(0);
            assertThat(singleTable.findGroup(single, null, 0)).isEqualTo(0);
            singleTable.endBatch();
        }
        finally {
            singleTable.releaseBuffers();
        }

        Vector[] composite = {utf8("alpha", "beta"), new I64Vector(new long[] {11, 12})};
        FlatGroupingTable compositeTable = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(composite, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                2,
                true);
        try {
            assertThat(compositeTable.usesIntHashRecords()).isFalse();
            compositeTable.beginBatch(composite, null);
            assertThat(compositeTable.assignGroup(composite, null, 0, 0)).isEqualTo(0);
            assertThat(compositeTable.findGroup(composite, null, 0)).isEqualTo(0);
            compositeTable.endBatch();
        }
        finally {
            compositeTable.releaseBuffers();
        }
    }

    @Test
    void testNullFreeLongBinarySpecializationSupportsEitherFieldOrder()
    {
        Vector[] binaryLong = {
                utf8("alpha", "beta", "alpha", "alpha"),
                new I64Vector(new long[] {11, 12, 11, 13})};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(binaryLong, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                4,
                true);
        try {
            table.beginBatch(binaryLong, new Vector[] {null, null});
            table.prepareBatchHashes(binaryLong, new Vector[] {null, null}, Mask.all(4));
            assertThat(table.assignGroup(binaryLong, null, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(binaryLong, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(binaryLong, null, 2, 2)).isEqualTo(0);
            assertThat(table.assignGroup(binaryLong, null, 3, 2)).isEqualTo(2);
            table.endBatch();

            Vector[] later = {
                    utf8("beta", "alpha", "gamma"),
                    new I64Vector(new long[] {12, 13, 14})};
            table.beginBatch(later, new Vector[] {null, null});
            table.prepareBatchHashes(later, new Vector[] {null, null}, Mask.all(3));
            assertThat(table.assignGroup(later, null, 0, 3)).isEqualTo(1);
            assertThat(table.assignGroup(later, null, 1, 3)).isEqualTo(2);
            assertThat(table.assignGroup(later, null, 2, 3)).isEqualTo(3);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testGeneratedDirectCompositeBatchSupportsArbitraryFieldCount()
    {
        Vector[] values = {
                DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1}, 5, utf8("alpha", "beta", "gamma")),
                new I64Vector(new long[] {1, 2, 1, 3, 2}),
                DictionaryVector.wrap(new int[] {1, 0, 1, 2, 0}, 5, utf8("left", "right", "center")),
                new I64Vector(new long[] {10, 11, 10, 12, 11})};
        Vector[] nulls = {null, null, null, null};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                values[0].length(),
                true);
        try {
            I64Vector groups = new I64Vector(values[0].length());
            Mask mask = Mask.all(values[0].length());
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, mask);
            long nextGroupId = table.assignDirectCompositeBatch(values, nulls, mask, groups, 0);
            table.endBatch();

            assertThat(nextGroupId).isEqualTo(3);
            assertThat(groups.values()).containsExactly(0, 1, 0, 2, 1);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testCompositeGroupingSupportsChangingIntegerEncodings()
    {
        Vector[] initial = {
                new I64Vector(new long[] {10, 20, 10, 30}),
                new I32Vector(new int[] {1, 2, 1, 3}),
                new I64Vector(new long[] {7, 7, 7, 7})};
        Vector[] nulls = {null, null, null};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(initial, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                initial[0].length(),
                true);
        try {
            I64Vector groups = new I64Vector(initial[0].length());
            Mask mask = Mask.all(initial[0].length());
            table.beginBatch(initial, nulls);
            table.prepareBatchHashes(initial, nulls, mask);
            long nextGroupId = 0;
            for (int position = 0; position < initial[0].length(); position++) {
                groups.values()[position] = table.assignGroup(initial, nulls, position, nextGroupId);
                if (groups.values()[position] == nextGroupId) {
                    nextGroupId++;
                }
            }
            table.endBatch();

            assertThat(nextGroupId).isEqualTo(3);
            assertThat(groups.values()).containsExactly(0, 1, 0, 2);

            DictionaryVector innerLongs = DictionaryVector.wrapNested(
                    new int[] {2, 0, 1},
                    3,
                    new I64Vector(new long[] {10, 20, 30}));
            Vector[] encoded = {
                    DictionaryVector.wrapNested(new int[] {1, 0, 2, 1}, 4, innerLongs),
                    DictionaryVector.wrap(new int[] {0, 2, 1, 0}, 4, new I32Vector(new int[] {1, 2, 3})),
                    new RleVector(new int[] {4}, new I64Vector(new long[] {7}))};
            table.beginBatch(encoded, nulls);
            table.prepareBatchHashes(encoded, nulls, mask);
            for (int position = 0; position < encoded[0].length(); position++) {
                groups.values()[position] = table.assignGroup(encoded, nulls, position, nextGroupId);
                if (groups.values()[position] == nextGroupId) {
                    nextGroupId++;
                }
            }
            table.endBatch();

            assertThat(nextGroupId).isEqualTo(3);
            assertThat(groups.values()).containsExactly(0, 2, 1, 0);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testPackedIdentityAdmissionRequiresAnotherInputBatch()
    {
        int size = 4096;
        long[] keys = new long[size];
        String[] labels = new String[size];
        for (int index = 0; index < size; index++) {
            keys[index] = index;
            labels[index] = "value-" + index;
        }
        Vector[] values = {new I64Vector(keys), utf8(labels)};
        Vector[] nulls = {null, null};

        GroupingState oneBatch = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            oneBatch.assignGroups(values, nulls, Mask.all(size), new I64Vector(size), false);
            assertThat(oneBatch.usesPackedFlatIdentitySlots()).isFalse();
        }
        finally {
            oneBatch.releaseBuffers();
        }

        GroupingState sustained = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            sustained.assignGroups(values, nulls, Mask.all(size), new I64Vector(size), true);
            assertThat(sustained.usesPackedFlatIdentitySlots()).isTrue();
        }
        finally {
            sustained.releaseBuffers();
        }
    }

    @Test
    void testFullWidthPairIdentityKeepsSignedIntDomainCompact()
    {
        int size = 1 << 10;
        long[] first = new long[size];
        long[] second = new long[size];
        for (int position = 0; position < size; position++) {
            first[position] = position;
            second[position] = position * 3L;
        }

        GroupingState compact = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            compact.assignGroups(
                    new Vector[] {new I64Vector(first), new I64Vector(second)},
                    new Vector[] {null, null},
                    Mask.all(size),
                    new I64Vector(size));
            assertThat(compact.usesPackedFlatIdentitySlots()).isFalse();
            assertThat(compact.groupCount()).isEqualTo(size);
        }
        finally {
            compact.releaseBuffers();
        }

        long[] wideFirst = first.clone();
        for (int position = 0; position < size; position++) {
            wideFirst[position] += 1L << 40;
        }
        GroupingState fullWidth = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            fullWidth.assignGroups(
                    new Vector[] {new I64Vector(wideFirst), new I64Vector(second)},
                    new Vector[] {null, null},
                    Mask.all(size),
                    new I64Vector(size));
            assertThat(fullWidth.usesPackedFlatIdentitySlots()).isTrue();
            assertThat(fullWidth.groupCount()).isEqualTo(size);
        }
        finally {
            fullWidth.releaseBuffers();
        }
    }

    @Test
    void testPrefetchedPackedIdentityPreservesSourceOrderForRepeatedNewKeys()
    {
        int size = 1 << 10;
        long[] first = new long[size];
        long[] second = new long[size];
        for (int position = 0; position < size; position++) {
            first[position] = (1L << 40) + position;
            second[position] = position * 3L;
        }

        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            I64Vector initialGroups = new I64Vector(size);
            state.assignGroups(
                    new Vector[] {new I64Vector(first), new I64Vector(second)},
                    new Vector[] {null, null},
                    Mask.all(size),
                    initialGroups);
            assertThat(state.usesPackedFlatIdentitySlots()).isTrue();
            assertThat(initialGroups.values()).containsExactly(Arrays.stream(first).map(value -> value - (1L << 40)).toArray());

            long newFirst = (1L << 50) + 7;
            long newSecond = (1L << 51) + 11;
            Vector[] repeatedNewKeys = {
                    new I64Vector(new long[] {newFirst, newFirst, first[17], newSecond, newSecond}),
                    new I64Vector(new long[] {31, 31, second[17], 47, 47})};
            I64Vector groups = new I64Vector(5);
            state.assignGroups(repeatedNewKeys, new Vector[] {null, null}, Mask.all(5), groups);

            assertThat(groups.values()).containsExactly(size, size, 17, size + 1, size + 1);
            assertThat(state.groupCount()).isEqualTo(size + 2);
        }
        finally {
            state.releaseBuffers();
        }
    }

    @Test
    void testPrefetchedGeneralFlatTablePreservesSourceOrderForRepeatedNewKeys()
    {
        Vector[] values = {
                new I64Vector(new long[] {7, 7, 8, 9, 9}),
                new F64Vector(new double[] {1.5, 1.5, 2.5, 3.5, 3.5}),
                new BooleanVector(new boolean[] {true, true, false, true, true})};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                4,
                false);
        try {
            I64Vector groups = new I64Vector(5);
            Vector[] nulls = {null, null, null};
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, Mask.all(5));
            assertThat(table.assignPrefetchedBatch(values, nulls, Mask.all(5), groups, 0)).isEqualTo(3);

            assertThat(groups.values()).containsExactly(0, 0, 1, 2, 2);
            assertThat(table.recordCount()).isEqualTo(3);
            assertThat(table.usesPackedHashRecordSlots()).isFalse();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testRejectedPackedSlotsPreserveNonIdentityBinaryRecordLayout()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("rejected-packed-binary-layout");
        Vector[] values = {utf8("alpha", "beta", "gamma")};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                4,
                false,
                true);
        try {
            table.beginBatch(values, new Vector[] {null});
            I64Vector groups = new I64Vector(3);
            groups.values()[0] = table.assignGroup(values, new Vector[] {null}, 0, 7);
            groups.values()[1] = table.assignGroup(values, new Vector[] {null}, 1, 8);
            groups.values()[2] = table.assignGroup(values, new Vector[] {null}, 2, 9);
            table.endBatch();

            assertThat(table.usesPackedHashRecordSlots()).isFalse();
            assertThat(groups.values()).containsExactly(7, 8, 9);
            Streams output = table.copyGroupedValuePositions(0, null, new int[] {7, 8, 9}, 0, 3, 0, 10, allocator, context);
            assertThat(OperatorVectorSupport.binaryEquals(output.values(), 0, "alpha".getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(OperatorVectorSupport.binaryEquals(output.values(), 1, "beta".getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(OperatorVectorSupport.binaryEquals(output.values(), 2, "gamma".getBytes(StandardCharsets.UTF_8))).isTrue();
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testNestedDictionaryNullMappingsAreRecomposedAtBatchBoundaries()
    {
        Vector[] values = {
                new I64Vector(new long[] {7, 7, 7, 7, 7}),
                new I64Vector(new long[] {1, 1, 1, 1, 1}),
                new I64Vector(new long[] {2, 2, 2, 2, 2}),
                new I64Vector(new long[] {3, 3, 3, 3, 3}),
                new I64Vector(new long[] {4, 4, 4, 4, 4}),
                new I64Vector(new long[] {5, 5, 5, 5, 5})};
        DictionaryVector innerNulls = DictionaryVector.wrapNested(
                new int[] {2, 1, 0, 1},
                4,
                new BooleanVector(new boolean[] {false, true, false}));
        Vector[] firstNulls = {DictionaryVector.wrapNested(new int[] {3, 0, 1, 2, 3}, 5, innerNulls), null, null, null, null, null};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 4, true);
        try {
            table.beginBatch(values, firstNulls);
            table.prepareBatchHashes(values, firstNulls, Mask.all(5));
            assertThat(table.assignGroup(values, firstNulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(values, firstNulls, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(values, firstNulls, 2, 2)).isEqualTo(0);
            assertThat(table.assignGroup(values, firstNulls, 3, 2)).isEqualTo(1);
            assertThat(table.assignGroup(values, firstNulls, 4, 2)).isEqualTo(0);
            table.endBatch();

            // Reuse the same layout with a shorter, differently ordered outer mapping. The high-water scratch
            // must be overwritten for the active positions rather than retaining the preceding batch's mapping.
            Vector[] secondValues = {
                    new I64Vector(new long[] {7, 7, 7}),
                    new I64Vector(new long[] {1, 1, 1}),
                    new I64Vector(new long[] {2, 2, 2}),
                    new I64Vector(new long[] {3, 3, 3}),
                    new I64Vector(new long[] {4, 4, 4}),
                    new I64Vector(new long[] {5, 5, 5})};
            Vector[] secondNulls = {DictionaryVector.wrapNested(new int[] {0, 1, 2}, 3, innerNulls), null, null, null, null, null};
            table.beginBatch(secondValues, secondNulls);
            table.prepareBatchHashes(secondValues, secondNulls, Mask.all(3));
            assertThat(table.assignGroup(secondValues, secondNulls, 0, 2)).isEqualTo(1);
            assertThat(table.assignGroup(secondValues, secondNulls, 1, 2)).isEqualTo(0);
            assertThat(table.assignGroup(secondValues, secondNulls, 2, 2)).isEqualTo(1);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testCompactEmbeddedBinaryAdmissionRejectsTinyAndUnreusedLayouts()
    {
        int size = 1024;
        Vector[] tinyCube = new Vector[8];
        for (int field = 0; field < 5; field++) {
            tinyCube[field] = DictionaryVector.wrap(compactTestIds(size, false), size, utf8("a", "b", "c", "d"));
        }
        tinyCube[5] = compactTestLongs(size, 1);
        tinyCube[6] = compactTestLongs(size, 10);
        tinyCube[7] = compactTestLongs(size, 100);
        assertThat(FlatKeyLayout.tryCreate(tinyCube, arrayPool, codeGeneration, flatKeyTablePolicy).usesCompactEmbeddedBinaryRecords()).isFalse();

        Vector[] unreused = new Vector[8];
        for (int field = 0; field < 5; field++) {
            unreused[field] = DictionaryVector.wrap(compactTestIds(size, false, 64), size, utf8(compactTestStrings("field-" + field + '-', 64)));
        }
        unreused[5] = compactTestLongs(size, 1);
        unreused[6] = compactTestLongs(size, 10);
        unreused[7] = compactTestLongs(size, 100);
        assertThat(FlatKeyLayout.tryCreate(unreused, arrayPool, codeGeneration, flatKeyTablePolicy).usesCompactEmbeddedBinaryRecords()).isFalse();

        Vector[] flat = {
                utf8(compactTestFlatStrings("category-", size, 10)),
                utf8(compactTestFlatStrings("class-", size, 20)),
                utf8(compactTestFlatStrings("brand-", size, 64)),
                utf8(compactTestFlatStrings("product-", size, size)),
                utf8(compactTestFlatStrings("store-", size, 12)),
                compactTestLongs(size, 1),
                compactTestLongs(size, 10),
                compactTestLongs(size, 100)};
        assertThat(FlatKeyLayout.tryCreate(flat, arrayPool, codeGeneration, flatKeyTablePolicy).usesCompactEmbeddedBinaryRecords()).isTrue();
    }

    @Test
    void testCompactEmbeddedBinaryRecordsInternAdmittedFlatFieldsAcrossPhysicalEncodings()
    {
        int size = 1024;
        String[][] strings = {
                compactTestFlatStrings("category-", size, 10),
                compactTestFlatStrings("class-", size, 20),
                compactTestFlatStrings("brand-", size, 64),
                compactTestFlatStrings("product-", size, size),
                compactTestFlatStrings("store-", size, 12)};
        long[] groupIds = new long[size];
        for (int position = 0; position < size; position++) {
            groupIds[position] = position;
        }
        for (String[] field : strings) {
            field[size - 1] = field[0];
        }
        groupIds[size - 1] = groupIds[0];

        Vector[] flat = {
                utf8(strings[0]),
                utf8(strings[1]),
                utf8(strings[2]),
                utf8(strings[3]),
                utf8(strings[4]),
                new I64Vector(groupIds)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(flat, arrayPool, codeGeneration, flatKeyTablePolicy);
        assertThat(layout.usesCompactEmbeddedBinaryRecords()).isTrue();
        assertThat(layout.fixedRecordSize()).isEqualTo(5 * Integer.BYTES + Long.BYTES);

        FlatGroupingTable table = new FlatGroupingTable(layout, 1024, true);
        try {
            table.beginBatch(flat, null);
            table.prepareBatchHashes(flat, null, Mask.all(size));
            long nextGroup = 0;
            for (int position = 0; position < size; position++) {
                long group = table.assignGroup(flat, null, position, nextGroup);
                if (group == nextGroup) {
                    nextGroup++;
                }
                if (position == size - 1) {
                    assertThat(group).isZero();
                }
            }
            assertThat(nextGroup).isEqualTo(size - 1);
            table.endBatch();

            Vector[] dictionaryProbe = {
                    DictionaryVector.wrap(new int[] {0}, 1, utf8(strings[0][0])),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8(strings[1][0])),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8(strings[2][0])),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8(strings[3][0])),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8(strings[4][0])),
                    new I64Vector(new long[] {groupIds[0]})};
            table.beginBatch(dictionaryProbe, null);
            assertThat(table.findGroup(dictionaryProbe, null, 0)).isZero();
            table.endBatch();

            Vector[] rleProbe = {
                    new RleVector(new int[] {1}, utf8(strings[0][0])),
                    new RleVector(new int[] {1}, utf8(strings[1][0])),
                    new RleVector(new int[] {1}, utf8(strings[2][0])),
                    new RleVector(new int[] {1}, utf8(strings[3][0])),
                    new RleVector(new int[] {1}, utf8(strings[4][0])),
                    new RleVector(new int[] {1}, new I64Vector(new long[] {groupIds[0]}))};
            table.beginBatch(rleProbe, null);
            assertThat(table.findGroup(rleProbe, null, 0)).isZero();
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testCompactEmbeddedBinaryRecordsPreserveIdsFallbacksAndMaterialization()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("compact-embedded-binary-records");
        int size = 1024;
        Vector[] first = {
                DictionaryVector.wrap(compactTestIds(size, false, 32), size, utf8(compactTestStrings())),
                DictionaryVector.wrap(compactTestIds(size, true), size, utf8("x", "u", "v", "w")),
                DictionaryVector.wrap(compactTestIds(size, false), size, utf8("m", "n", "o", "r")),
                DictionaryVector.wrap(compactTestIds(size, false), size, utf8("p", "q", "s", "t")),
                DictionaryVector.wrap(compactTestIds(size, true), size, utf8("z", "y", "j", "k")),
                compactTestLongs(size, 1),
                compactTestLongs(size, 10),
                compactTestLongs(size, 100)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(first, arrayPool, codeGeneration, flatKeyTablePolicy);
        assertThat(layout.usesCompactEmbeddedBinaryRecords()).isTrue();
        assertThat(layout.fixedRecordSize()).isEqualTo(5 * Integer.BYTES + 3 * Long.BYTES);

        FlatGroupingTable table = new FlatGroupingTable(layout, 4, true);
        try {
            table.beginBatch(first, null);
            assertThat(layout.usesGeneratedDictionaryRecordEquality()).isTrue();
            assertThat(table.assignGroup(first, null, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(first, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(first, null, 2, 2)).isEqualTo(0);
            table.endBatch();

            // Reusable flat values receive query-stable ids when a record is created. Existing dictionary-created
            // records still compare by exact value, and a new record keeps the same compact representation.
            Vector[] flat = {
                    utf8("b", "c"),
                    utf8("x", "x"),
                    utf8("n", "n"),
                    utf8("q", "q"),
                    utf8("z", "z"),
                    new I64Vector(new long[] {2, 3}),
                    new I64Vector(new long[] {20, 30}),
                    new I64Vector(new long[] {200, 300})};
            table.beginBatch(flat, null);
            assertThat(table.assignGroup(flat, null, 0, 2)).isEqualTo(1);
            assertThat(table.assignGroup(flat, null, 1, 2)).isEqualTo(2);
            table.endBatch();

            // A later dictionary-backed probe must find the record that was written through the fallback path.
            Vector[] dictionaryAgain = {
                    DictionaryVector.wrap(new int[] {0}, 1, utf8("c")),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8("x")),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8("n")),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8("q")),
                    DictionaryVector.wrap(new int[] {0}, 1, utf8("z")),
                    new I64Vector(new long[] {3}),
                    new I64Vector(new long[] {30}),
                    new I64Vector(new long[] {300})};
            table.beginBatch(dictionaryAgain, null);
            assertThat(layout.usesGeneratedDictionaryRecordEquality()).isTrue();
            assertThat(table.findGroup(dictionaryAgain, null, 0)).isEqualTo(2);
            table.endBatch();

            String[][] expected = {
                    {"a", "b", "c"},
                    {"x", "x", "x"},
                    {"m", "n", "n"},
                    {"p", "q", "q"},
                    {"z", "z", "z"}};
            for (int field = 0; field < expected.length; field++) {
                Vector grouped = table.groupedValues(field, Mask.all(3), null, allocator, context).values();
                for (int group = 0; group < expected[field].length; group++) {
                    assertThat(OperatorVectorSupport.binaryEquals(grouped, group, expected[field][group].getBytes(StandardCharsets.UTF_8))).isTrue();
                }

                Streams copied = table.copyGroupedValuePositions(
                        field,
                        null,
                        new int[] {2, 0, 1},
                        0,
                        3,
                        0,
                        3,
                        allocator,
                        context);
                assertThat(copied.getOrNull(org.weakref.nitro.data.Stream.NULLS)).isNotNull();
                assertThat(OperatorVectorSupport.binaryEquals(copied.values(), 0, expected[field][2].getBytes(StandardCharsets.UTF_8))).isTrue();
                assertThat(OperatorVectorSupport.binaryEquals(copied.values(), 1, expected[field][0].getBytes(StandardCharsets.UTF_8))).isTrue();
                assertThat(OperatorVectorSupport.binaryEquals(copied.values(), 2, expected[field][1].getBytes(StandardCharsets.UTF_8))).isTrue();
            }
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testSingleDictionaryGroupCacheObservesImmutablePooledContentGeneration()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("dictionary-group-cache-generation");
        BinaryVector first = binary(allocator, context, "alpha", "beta").freezeContent();
        Vector[] firstValues = {DictionaryVector.wrap(new int[] {0, 1, 0, 1}, 4, first)};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(firstValues, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        try {
            table.beginBatch(firstValues, new Vector[] {null});
            table.prepareBatchHashes(firstValues, new Vector[] {null}, Mask.all(4));
            assertThat(table.singleDictionaryGroupCacheActive()).isTrue();
            assertThat(table.assignGroupCached(firstValues, null, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroupCached(firstValues, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroupCached(firstValues, null, 2, 2)).isEqualTo(0);
            assertThat(table.assignGroupCached(firstValues, null, 3, 2)).isEqualTo(1);
            table.endBatch();

            Vector[] repeated = {DictionaryVector.wrap(new int[] {1, 0}, 2, first)};
            table.beginBatch(repeated, new Vector[] {null});
            table.prepareBatchHashes(repeated, new Vector[] {null}, Mask.all(2));
            assertThat(table.singleDictionaryGroupCacheActive()).isTrue();
            assertThat(table.assignGroupCached(repeated, null, 0, 2)).isEqualTo(1);
            assertThat(table.assignGroupCached(repeated, null, 1, 2)).isEqualTo(0);
            table.endBatch();

            allocator.release(context, first);
            BinaryVector reused = binary(allocator, context, "gamma", "zeta").freezeContent();
            assertThat(reused).isSameAs(first);
            Vector[] changed = {DictionaryVector.wrap(new int[] {0, 1}, 2, reused)};
            table.beginBatch(changed, new Vector[] {null});
            table.prepareBatchHashes(changed, new Vector[] {null}, Mask.all(2));
            assertThat(table.singleDictionaryGroupCacheActive()).isTrue();
            assertThat(table.assignGroupCached(changed, null, 0, 2)).isEqualTo(2);
            assertThat(table.assignGroupCached(changed, null, 1, 3)).isEqualTo(3);
            table.endBatch();

            allocator.release(context, reused);
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testSingleDictionaryGroupCacheRejectsSparseBatch()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("sparse-dictionary-group-cache");
        BinaryVector dictionary = binary(allocator, context, "alpha", "beta");
        Vector[] values = {DictionaryVector.wrap(new int[] {0, 1, 0, 1}, 4, dictionary)};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        try {
            table.beginBatch(values, new Vector[] {null});
            table.prepareBatchHashes(values, new Vector[] {null}, Mask.sparse(new int[] {0, 2}, 4));
            assertThat(table.singleDictionaryGroupCacheActive()).isFalse();
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testDictionaryValueIdsObservePooledBinaryGeneration()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("pooled-dictionary-generation");
        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        int[] firstId = {0};
        int[] secondId = {1};

        BinaryVector first = binary(allocator, context, "alpha", "beta");
        I64Vector firstGroups = new I64Vector(1);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(firstId, 1, first)},
                new Vector[] {null},
                Mask.all(1),
                firstGroups);
        assertThat(firstGroups.values()).containsExactly(0);
        I64Vector repeatedGenerationGroups = new I64Vector(1);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(secondId, 1, first)},
                new Vector[] {null},
                Mask.all(1),
                repeatedGenerationGroups);
        assertThat(repeatedGenerationGroups.values()).containsExactly(1);
        allocator.release(context, first);

        BinaryVector second = binary(allocator, context, "gamma", "zeta");
        assertThat(second).isSameAs(first);
        I64Vector secondGroups = new I64Vector(1);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(firstId, 1, second)},
                new Vector[] {null},
                Mask.all(1),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(2);
        I64Vector secondRepeatedGenerationGroups = new I64Vector(1);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(secondId, 1, second)},
                new Vector[] {null},
                Mask.all(1),
                secondRepeatedGenerationGroups);
        assertThat(secondRepeatedGenerationGroups.values()).containsExactly(3);
        assertThat(state.groupCount()).isEqualTo(4);

        allocator.release(context, second);
        state.releaseBuffers();
        allocator.release(context);
    }

    @Test
    void testSingleLongDictionaryGroupsReuseDictionaryEntries()
    {
        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        I64Vector groups = new I64Vector(8);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(new int[] {0, 1, 0, 2, 1, 2, 0, 1}, 8, new I64Vector(new long[] {11, 22, 33}))},
                new Vector[] {null},
                Mask.all(8),
                groups);

        assertThat(state.usesSingleLongGrouping()).isTrue();
        assertThat(state.groupCount()).isEqualTo(3);
        assertThat(groups.values()).containsExactly(0, 1, 0, 2, 1, 2, 0, 1);

        I64Vector repeated = new I64Vector(4);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(new int[] {2, 0, 1, 2}, 4, new I64Vector(new long[] {11, 22, 33}))},
                new Vector[] {null},
                Mask.all(4),
                repeated);
        assertThat(repeated.values()).containsExactly(2, 0, 1, 2);
        assertThat(state.groupCount()).isEqualTo(3);
        state.releaseBuffers();
    }

    @Test
    void testSingleFlatDictionaryGroupsReuseEntriesAndKeepNullOutsideTable()
    {
        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        I64Vector firstGroups = new I64Vector(7);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(
                        new int[] {0, 1, 0, 2, 1, 2, 0},
                        7,
                        new F64Vector(new double[] {1.5, 2.5, 3.5}))},
                new Vector[] {new BooleanVector(new boolean[] {false, false, false, false, true, false, false})},
                Mask.all(7),
                firstGroups);

        assertThat(state.usesSingleLongGrouping()).isFalse();
        assertThat(state.groupCount()).isEqualTo(4);
        assertThat(firstGroups.values()).containsExactly(0, 1, 0, 2, 3, 2, 0);

        I64Vector secondGroups = new I64Vector(5);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(
                        new int[] {0, 1, 2, 0, 1},
                        5,
                        new F64Vector(new double[] {3.5, 4.5, 1.5}))},
                new Vector[] {null},
                Mask.all(5),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(2, 4, 0, 2, 4);
        assertThat(state.groupCount()).isEqualTo(5);
        state.releaseBuffers();
    }

    @Test
    void testSingleFlatDictionaryCanGroupWithoutMaterializingLogicalGroupIds()
    {
        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        int[] ids = new int[1_000];
        boolean[] nulls = new boolean[ids.length];
        for (int position = 0; position < ids.length; position++) {
            ids[position] = position % 3;
        }
        nulls[3] = true;

        assertThat(state.assignGroupsDiscardingResults(
                new Vector[] {DictionaryVector.wrap(ids, new F64Vector(new double[] {1.5, 2.5, 3.5}))},
                new Vector[] {new BooleanVector(nulls)},
                Mask.all(ids.length)))
                .isTrue();
        assertThat(state.groupCount()).isEqualTo(4);

        I64Vector groups = new I64Vector(4);
        state.assignGroups(
                new Vector[] {new F64Vector(new double[] {3.5, 1.5, 4.5, 0})},
                new Vector[] {new BooleanVector(new boolean[] {false, false, false, true})},
                Mask.all(4),
                groups);
        assertThat(groups.values()).containsExactly(2, 0, 4, 3);
        state.releaseBuffers();
    }

    @Test
    void testDictionaryValueIdsObserveInPlaceBinaryRefillGeneration()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("in-place-dictionary-generation");
        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);

        BinaryVector dictionary = binary(allocator, context, "alpha", "beta");
        I64Vector firstGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(new int[] {0, 1}, 2, dictionary)},
                new Vector[] {null},
                Mask.all(2),
                firstGroups);
        assertThat(firstGroups.values()).containsExactly(0, 1);

        long previousGeneration = dictionary.contentGeneration();
        BinaryVector refilled = BinaryVector.allocateOrGrow(allocator, context, dictionary, 2, dictionary.byteCapacity());
        assertThat(refilled).isSameAs(dictionary);
        assertThat(refilled.contentGeneration()).isGreaterThan(previousGeneration);
        refilled.setBytes(0, "gamma".getBytes(StandardCharsets.UTF_8));
        refilled.setBytes(1, "zeta".getBytes(StandardCharsets.UTF_8));

        I64Vector secondGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(new int[] {0, 1}, 2, refilled)},
                new Vector[] {null},
                Mask.all(2),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(2, 3);

        previousGeneration = refilled.contentGeneration();
        refilled.setBytes(0, "delta".getBytes(StandardCharsets.UTF_8));
        refilled.setBytes(1, "iota".getBytes(StandardCharsets.UTF_8));
        assertThat(refilled.contentGeneration()).isGreaterThan(previousGeneration);
        I64Vector thirdGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(new int[] {0, 1}, 2, refilled)},
                new Vector[] {null},
                Mask.all(2),
                thirdGroups);
        assertThat(thirdGroups.values()).containsExactly(4, 5);

        allocator.release(context, refilled);
        state.releaseBuffers();
        allocator.release(context);
    }

    @Test
    void testDictionaryValueIdsDoNotCacheMutableRawBinaryContent()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("mutable-raw-dictionary-content");
        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);

        BinaryVector dictionary = binary(allocator, context, "alpha", "beta");
        I64Vector firstGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(new int[] {0, 1}, 2, dictionary)},
                new Vector[] {null},
                Mask.all(2),
                firstGroups);
        assertThat(firstGroups.values()).containsExactly(0, 1);

        long generation = dictionary.contentGeneration();
        System.arraycopy("gamma".getBytes(StandardCharsets.UTF_8), 0, dictionary.data(), dictionary.startOffset(0), 5);
        System.arraycopy("zeta".getBytes(StandardCharsets.UTF_8), 0, dictionary.data(), dictionary.startOffset(1), 4);
        assertThat(dictionary.contentGeneration()).isEqualTo(generation);

        I64Vector secondGroups = new I64Vector(2);
        state.assignGroups(
                new Vector[] {DictionaryVector.wrap(new int[] {0, 1}, 2, dictionary)},
                new Vector[] {null},
                Mask.all(2),
                secondGroups);
        assertThat(secondGroups.values()).containsExactly(2, 3);

        allocator.release(context, dictionary);
        state.releaseBuffers();
        allocator.release(context);
    }

    @Test
    void testNormalizedScratchRequiresProportionalAddressSpace()
    {
        FlatKeyTablePolicy.Table policy = flatKeyTablePolicy.table();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(policy, 1024, 1024)).isTrue();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(policy, 1024, 256)).isTrue();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(policy, 1024, 255)).isFalse();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(policy, 127, 127)).isFalse();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(policy, 1 << 26, 2)).isFalse();
        assertThat(FlatGroupingTable.shouldPrepareNormalizedScratch(policy, 0, 0)).isFalse();
    }

    @Test
    void testDictionaryEntryHashReusePreservesWideHybridAccessors()
    {
        assertThat(FlatKeyLayout.shouldReuseDictionaryEntryHashes(2, true, false, flatKeyTablePolicy.layout())).isTrue();
        assertThat(FlatKeyLayout.shouldReuseDictionaryEntryHashes(5, true, false, flatKeyTablePolicy.layout())).isFalse();
        assertThat(FlatKeyLayout.shouldReuseDictionaryEntryHashes(5, false, false, flatKeyTablePolicy.layout())).isTrue();
        assertThat(FlatKeyLayout.shouldReuseDictionaryEntryHashes(5, true, true, flatKeyTablePolicy.layout())).isTrue();
    }

    @Test
    void testEnsureCapacityPreservesExistingAndLaterGroups()
    {
        Vector[] values = {utf8("alpha", "beta", "alpha")};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(values, arrayPool, codeGeneration, flatKeyTablePolicy), 2);
        try {
            table.beginBatch(values, new Vector[] {null});
            assertThat(table.assignGroup(values, null, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(values, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(values, null, 2, 2)).isEqualTo(0);
            table.endBatch();

            table.ensureCapacity(1 << 16);

            Vector[] later = {utf8("beta", "gamma", "alpha")};
            table.beginBatch(later, new Vector[] {null});
            assertThat(table.findGroup(later, null, 0)).isEqualTo(1);
            assertThat(table.assignGroup(later, null, 1, 2)).isEqualTo(2);
            assertThat(table.findGroup(later, null, 2)).isEqualTo(0);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testSingleRunBinaryKeysPreserveCompositeEqualityAcrossBatches()
    {
        Vector[] first = {
                new RleVector(new int[] {4}, utf8("web")),
                new RleVector(new int[] {4}, utf8("sales")),
                new I64Vector(new long[] {2000, 2000, 2001, 2000}),
                new I64Vector(new long[] {1, 1, 1, 2}),
                utf8("books", "books", "books", "books")};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(first, arrayPool, codeGeneration, flatKeyTablePolicy), 4);
        try {
            table.beginBatch(first, null);
            table.prepareBatchHashes(first, null, Mask.all(4));
            assertThat(table.assignGroup(first, null, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(first, null, 1, 1)).isEqualTo(0);
            assertThat(table.assignGroup(first, null, 2, 1)).isEqualTo(1);
            assertThat(table.assignGroup(first, null, 3, 2)).isEqualTo(2);
            table.endBatch();

            // The accessor is batch-local: changing either single-run value must not reuse the preceding batch's
            // cached entry or hash, while an identical later batch must still find the original exact record.
            Vector[] changed = {
                    new RleVector(new int[] {2}, utf8("store")),
                    new RleVector(new int[] {2}, utf8("returns")),
                    new I64Vector(new long[] {2000, 2001}),
                    new I64Vector(new long[] {1, 1}),
                    utf8("books", "books")};
            table.beginBatch(changed, null);
            table.prepareBatchHashes(changed, null, Mask.all(2));
            assertThat(table.assignGroup(changed, null, 0, 3)).isEqualTo(3);
            assertThat(table.assignGroup(changed, null, 1, 4)).isEqualTo(4);
            table.endBatch();

            Vector[] repeated = {
                    new RleVector(new int[] {1}, utf8("web")),
                    new RleVector(new int[] {1}, utf8("sales")),
                    new I64Vector(new long[] {2000}),
                    new I64Vector(new long[] {1}),
                    utf8("books")};
            table.beginBatch(repeated, null);
            table.prepareBatchHashes(repeated, null, Mask.all(1));
            assertThat(table.assignGroup(repeated, null, 0, 5)).isEqualTo(0);
            table.endBatch();

            Allocator allocator = new Allocator(EngineResources.createDefault());
            Allocator.Context context = new Allocator.Context("single-run-binary-grouped-output");
            try {
                Vector groupedChannel = table.groupedValues(0, Mask.all(5), null, allocator, context).values();
                Vector groupedColumn = table.groupedValues(1, Mask.all(5), null, allocator, context).values();
                assertThat(OperatorVectorSupport.binaryEquals(groupedChannel, 0, "web".getBytes(StandardCharsets.UTF_8))).isTrue();
                assertThat(OperatorVectorSupport.binaryEquals(groupedColumn, 0, "sales".getBytes(StandardCharsets.UTF_8))).isTrue();
                assertThat(OperatorVectorSupport.binaryEquals(groupedChannel, 3, "store".getBytes(StandardCharsets.UTF_8))).isTrue();
                assertThat(OperatorVectorSupport.binaryEquals(groupedColumn, 4, "returns".getBytes(StandardCharsets.UTF_8))).isTrue();
            }
            finally {
                allocator.release(context);
            }
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testDiscriminatingFieldHashStillComparesCompleteCompositeKey()
    {
        int size = 128;
        String[] first = new String[size];
        String[] second = new String[size];
        String[] third = new String[size];
        String[] fourth = new String[size];
        for (int position = 0; position < size; position++) {
            first[position] = "customer-" + position;
            second[position] = "first";
            third[position] = "last";
            fourth[position] = "country";
        }
        int[] sharedIds = new int[size];
        for (int position = 0; position < size; position++) {
            sharedIds[position] = position;
        }

        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        I64Vector initialGroups = new I64Vector(size);
        state.assignGroups(
                new Vector[] {
                        nestedDictionary(sharedIds, first),
                        nestedDictionary(sharedIds, second),
                        nestedDictionary(sharedIds, third),
                        nestedDictionary(sharedIds, fourth)},
                new Vector[] {null, null, null, null},
                Mask.all(size),
                initialGroups);
        assertThat(state.groupCount()).isEqualTo(size);

        // The high-cardinality first field is sufficient for hash placement, but never for equality. A later row
        // with the same discriminator and a different trailing field must remain a distinct composite group.
        fourth[0] = "different-country";
        I64Vector laterGroups = new I64Vector(size);
        state.assignGroups(
                new Vector[] {
                        nestedDictionary(sharedIds, first),
                        nestedDictionary(sharedIds, second),
                        nestedDictionary(sharedIds, third),
                        nestedDictionary(sharedIds, fourth)},
                new Vector[] {null, null, null, null},
                Mask.all(size),
                laterGroups);
        assertThat(state.groupCount()).isEqualTo(size + 1L);
        assertThat(laterGroups.values()[0]).isEqualTo(size);
        assertThat(laterGroups.values()[1]).isEqualTo(initialGroups.values()[1]);
        state.releaseBuffers();
    }

    @Test
    void testGeneratedDictionaryHashHonorsAdaptiveDiscriminatingField()
    {
        int size = 128;
        int[] sharedIds = new int[size];
        String[][] fields = new String[5][size];
        for (int position = 0; position < size; position++) {
            sharedIds[position] = position;
            fields[0][position] = "customer-" + position;
            fields[1][position] = "first";
            fields[2][position] = "last";
            fields[3][position] = "country";
            fields[4][position] = "region";
        }
        Vector[] values = {
                nestedDictionary(sharedIds, fields[0]),
                nestedDictionary(sharedIds, fields[1]),
                nestedDictionary(sharedIds, fields[2]),
                nestedDictionary(sharedIds, fields[3]),
                nestedDictionary(sharedIds, fields[4])};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, null);
            long[] expected = new long[size];
            for (int position = 0; position < size; position++) {
                expected[position] = layout.hash(values, null, position);
            }
            long[] actual = new long[size];
            assertThat(layout.prepareGeneratedDictionaryBatchHashes(size, actual)).isTrue();
            assertThat(actual).containsExactly(expected);
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedDictionaryHashCannotChangeEstablishedNormalizedHashStrategy()
    {
        int size = 128;
        int[] ids = new int[size];
        long[] repeated = new long[size];
        for (int position = 0; position < size; position++) {
            ids[position] = position % 2;
        }
        Vector[] first = {
                DictionaryVector.wrapNested(ids, size, utf8("alpha", "beta")),
                DictionaryVector.wrapNested(ids, size, utf8("first", "second")),
                new I64Vector(repeated)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(first, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(first, null);
            assertThat(layout.batchSupportsNormalizedIntKey()).isTrue();
            layout.establishHashStrategy();
            layout.endBatch();

            long[] distinct = new long[size];
            for (int position = 0; position < size; position++) {
                distinct[position] = position;
            }
            Vector[] later = {
                    DictionaryVector.wrapNested(ids, size, utf8("alpha", "beta")),
                    DictionaryVector.wrapNested(ids, size, utf8("first", "second")),
                    new I64Vector(distinct)};
            layout.beginBatch(later, null);
            // The two reusable binary lanes amortize normalization even when the integer lane becomes highly
            // discriminating. The already-established normalized strategy remains unchanged, so the alternate
            // generated dictionary-hash path must still stay disabled.
            assertThat(layout.batchSupportsNormalizedIntKey()).isTrue();
            assertThat(layout.prepareGeneratedDictionaryBatchHashes(size, new long[size])).isFalse();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testBatchedProbeCannotChangeScalarBuildHashStrategy()
    {
        int size = 128;
        int[] sharedIds = new int[size];
        String[][] fields = new String[4][size];
        for (int position = 0; position < size; position++) {
            sharedIds[position] = position;
            fields[0][position] = "customer-" + position;
            fields[1][position] = "first";
            fields[2][position] = "last";
            fields[3][position] = "country";
        }
        Vector[] values = {
                nestedDictionary(sharedIds, fields[0]),
                nestedDictionary(sharedIds, fields[1]),
                nestedDictionary(sharedIds, fields[2]),
                nestedDictionary(sharedIds, fields[3])};
        Vector[] nulls = {null, null, null, null};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                size,
                true);
        try {
            for (int position = 0; position < size; position++) {
                assertThat(table.assignGroup(values, nulls, position, position)).isEqualTo(position);
            }

            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, Mask.all(size));
            for (int position = 0; position < size; position++) {
                assertThat(table.findGroup(values, nulls, position)).isEqualTo(position);
            }
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testMixedCompositePreservesConstantNullGroupingSets()
    {
        int[] ids = {0, 0, 1, 1};
        Vector[] values = {
                DictionaryVector.wrapNested(ids, ids.length, utf8("CA", "NY")),
                DictionaryVector.wrapNested(ids, ids.length, utf8("A", "B")),
                new I64Vector(new long[] {0, 0, 0, 0})};
        BooleanVector allNull = new BooleanVector(new boolean[] {true, true, true, true});
        BooleanVector nullFree = new BooleanVector(new boolean[] {false, false, false, false});
        GroupingState state = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        try {
            I64Vector grandTotal = new I64Vector(ids.length);
            state.assignGroups(values, new Vector[] {allNull, allNull, nullFree}, Mask.all(ids.length), grandTotal);
            assertThat(grandTotal.values()).containsExactly(0, 0, 0, 0);

            ((I64Vector) values[2]).values()[0] = 1;
            ((I64Vector) values[2]).values()[1] = 1;
            ((I64Vector) values[2]).values()[2] = 1;
            ((I64Vector) values[2]).values()[3] = 1;
            I64Vector byFirst = new I64Vector(ids.length);
            state.assignGroups(values, new Vector[] {nullFree, allNull, nullFree}, Mask.all(ids.length), byFirst);
            assertThat(byFirst.values()).containsExactly(1, 1, 2, 2);

            ((I64Vector) values[2]).values()[0] = 2;
            ((I64Vector) values[2]).values()[1] = 2;
            ((I64Vector) values[2]).values()[2] = 2;
            ((I64Vector) values[2]).values()[3] = 2;
            I64Vector detail = new I64Vector(ids.length);
            state.assignGroups(values, new Vector[] {nullFree, nullFree, nullFree}, Mask.all(ids.length), detail);
            assertThat(detail.values()).containsExactly(3, 3, 4, 4);

            // A later batch with mixed nullness must leave the batch-constant path and preserve exact equality.
            BooleanVector mixedNulls = new BooleanVector(new boolean[] {false, true, false, true});
            I64Vector mixed = new I64Vector(ids.length);
            state.assignGroups(values, new Vector[] {mixedNulls, nullFree, nullFree}, Mask.all(ids.length), mixed);
            assertThat(mixed.values()).containsExactly(3, 5, 4, 6);
            assertThat(state.groupCount()).isEqualTo(7);
        }
        finally {
            state.releaseBuffers();
        }
    }

    private static DictionaryVector nestedDictionary(int[] ids, String[] values)
    {
        DictionaryVector inner = DictionaryVector.wrapNested(ids, ids.length, utf8(values));
        return DictionaryVector.wrapNested(ids, ids.length, inner);
    }

    private static int[] compactTestIds(int size, boolean firstThreeZero)
    {
        return compactTestIds(size, firstThreeZero, 4);
    }

    private static int[] compactTestIds(int size, boolean firstThreeZero, int cardinality)
    {
        int[] ids = new int[size];
        for (int position = 0; position < size; position++) {
            ids[position] = (position >>> 4) % cardinality;
        }
        ids[0] = 0;
        ids[1] = firstThreeZero ? 0 : 1;
        ids[2] = 0;
        return ids;
    }

    private static String[] compactTestStrings()
    {
        String[] values = new String[32];
        values[0] = "a";
        values[1] = "b";
        values[2] = "c";
        values[3] = "d";
        for (int index = 4; index < values.length; index++) {
            values[index] = "value-" + index;
        }
        return values;
    }

    private static String[] compactTestStrings(String prefix, int size)
    {
        String[] values = new String[size];
        for (int index = 0; index < size; index++) {
            values[index] = prefix + index;
        }
        return values;
    }

    private static String[] compactTestFlatStrings(String prefix, int size, int cardinality)
    {
        String[] values = new String[size];
        for (int position = 0; position < size; position++) {
            values[position] = prefix + (position % cardinality);
        }
        return values;
    }

    private static I64Vector compactTestLongs(int size, int scale)
    {
        long[] values = new long[size];
        for (int position = 0; position < size; position++) {
            values[position] = (long) (position + 1) * scale;
        }
        values[0] = scale;
        values[1] = 2L * scale;
        values[2] = scale;
        return new I64Vector(values);
    }

    private static BinaryVector binary(Allocator allocator, Allocator.Context context, String... values)
    {
        int byteCount = 0;
        for (String value : values) {
            byteCount += value.getBytes(StandardCharsets.UTF_8).length;
        }
        BinaryVector result = BinaryVector.allocate(allocator, context, values.length, byteCount);
        for (int position = 0; position < values.length; position++) {
            result.setBytes(position, values[position].getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    @Test
    void testRecordIdentityAdmissionRequiresLargeHighCardinalityBatch()
    {
        int size = 1 << 12;
        String[] distinctValues = new String[size];
        String[] categoricalValues = new String[size];
        for (int position = 0; position < size; position++) {
            distinctValues[position] = "key-" + position;
            categoricalValues[position] = "category-" + (position & 15);
        }

        GroupingState distinct = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        I64Vector distinctGroups = new I64Vector(size);
        distinct.assignGroups(new Vector[] {utf8(distinctValues)}, new Vector[] {null}, Mask.all(size), distinctGroups);
        assertThat(distinct.usesFlatSingleRecordIdentity()).isTrue();
        assertThat(distinct.usesPackedFlatIdentitySlots()).isFalse();
        assertThat(distinct.groupCount()).isEqualTo(size);
        assertThat(distinctGroups.values()[0]).isZero();
        assertThat(distinctGroups.values()[size - 1]).isEqualTo(size - 1L);
        distinct.releaseBuffers();

        GroupingState categorical = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        I64Vector categoricalGroups = new I64Vector(size);
        categorical.assignGroups(new Vector[] {utf8(categoricalValues)}, new Vector[] {null}, Mask.all(size), categoricalGroups);
        assertThat(categorical.usesFlatSingleRecordIdentity()).isFalse();
        assertThat(categorical.groupCount()).isEqualTo(16);
        assertThat(categoricalGroups.values()[0]).isEqualTo(categoricalGroups.values()[16]);
        categorical.releaseBuffers();

        String[] mostlyDistinctValues = new String[size];
        for (int position = 0; position < size; position++) {
            // The admission sample is 87.5% distinct: representative of a grouping stream with modest reuse.
            mostlyDistinctValues[position] = "mostly-" + (position < 224 ? position : position < 256 ? position - 224 : position);
        }
        GroupingState mostlyDistinct = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        I64Vector mostlyDistinctGroups = new I64Vector(size);
        mostlyDistinct.assignGroups(new Vector[] {utf8(mostlyDistinctValues)}, new Vector[] {null}, Mask.all(size), mostlyDistinctGroups);
        assertThat(mostlyDistinct.usesFlatSingleRecordIdentity()).isTrue();
        assertThat(mostlyDistinct.usesPackedFlatIdentitySlots()).isFalse();
        mostlyDistinct.releaseBuffers();
    }

    @Test
    void testDictionaryRecordIdentityStoresTopLevelNullInTable()
    {
        int size = 1 << 12;
        String[] distinctValues = new String[size];
        int[] ids = new int[size];
        for (int position = 0; position < size; position++) {
            distinctValues[position] = "key-" + position;
            ids[position] = position;
        }
        DictionaryVector dictionary = DictionaryVector.wrap(ids, utf8(distinctValues));
        BooleanVector nulls = new BooleanVector(size);
        nulls.values()[25] = true;

        GroupingState grouping = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        I64Vector groups = new I64Vector(size);
        grouping.assignGroups(new Vector[] {dictionary}, new Vector[] {nulls}, Mask.all(size), groups);

        assertThat(grouping.usesFlatSingleRecordIdentity()).isTrue();
        assertThat(grouping.groupCount()).isEqualTo(size);
        assertThat(groups.values()[24]).isEqualTo(24);
        assertThat(groups.values()[25]).isEqualTo(25);
        assertThat(groups.values()[26]).isEqualTo(26);
        assertThat(groups.values()[size - 1]).isEqualTo(size - 1L);
        grouping.releaseBuffers();
    }

    @Test
    void testRecordIdentityAdmissionDoesNotReplacePopulatedDictionaryTable()
    {
        GroupingState grouping = new GroupingState(arrayPool, codeGeneration, groupingResources, adaptiveLongGroupingPolicy, flatKeyTablePolicy);
        DictionaryVector dictionary = new DictionaryVector(
                new int[] {0, 1, 0, 1},
                utf8("dictionary-a", "dictionary-b"));
        I64Vector dictionaryGroups = new I64Vector(dictionary.length());
        grouping.assignGroups(new Vector[] {dictionary}, new Vector[] {null}, Mask.all(dictionary.length()), dictionaryGroups);

        int size = 1 << 12;
        String[] distinctValues = new String[size];
        for (int position = 0; position < size; position++) {
            distinctValues[position] = "flat-key-" + position;
        }
        I64Vector flatGroups = new I64Vector(size);
        grouping.assignGroups(new Vector[] {utf8(distinctValues)}, new Vector[] {null}, Mask.all(size), flatGroups);

        assertThat(grouping.usesFlatSingleRecordIdentity()).isFalse();
        assertThat(grouping.groupCount()).isEqualTo(size + 2);
        assertThat(dictionaryGroups.values()).containsExactly(0, 1, 0, 1);
        assertThat(flatGroups.values()[0]).isEqualTo(2);
        assertThat(flatGroups.values()[size - 1]).isEqualTo(size + 1L);
        grouping.releaseBuffers();
    }

    @Test
    void testNullableRecordIdentityPreservesNullOrder()
    {
        org.weakref.nitro.data.BooleanVector firstNulls = new org.weakref.nitro.data.BooleanVector(4);
        firstNulls.values()[1] = true;
        Vector[] nulls = {firstNulls};
        BinaryVector first = utf8("alpha", "ignored", "beta", "alpha");
        Vector[] firstValues = {first};
        FlatGroupingTable table = new FlatGroupingTable(FlatKeyLayout.tryCreate(firstValues, true, arrayPool, codeGeneration, flatKeyTablePolicy), 16, true);
        try {
            table.beginBatch(firstValues, nulls);
            assertThat(table.assignGroup(firstValues, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(firstValues, nulls, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(firstValues, nulls, 2, 2)).isEqualTo(2);
            assertThat(table.assignGroup(firstValues, nulls, 3, 3)).isEqualTo(0);
            table.endBatch();

            org.weakref.nitro.data.BooleanVector secondNulls = new org.weakref.nitro.data.BooleanVector(3);
            secondNulls.values()[1] = true;
            Vector[] secondNullVectors = {secondNulls};
            BinaryVector second = utf8("gamma", "ignored", "beta");
            Vector[] secondValues = {second};
            table.beginBatch(secondValues, secondNullVectors);
            assertThat(table.assignGroup(secondValues, secondNullVectors, 0, 3)).isEqualTo(3);
            assertThat(table.assignGroup(secondValues, secondNullVectors, 1, 4)).isEqualTo(1);
            assertThat(table.assignGroup(secondValues, secondNullVectors, 2, 4)).isEqualTo(2);
            table.endBatch();

            assertThat(table.recordIndex(0)).isEqualTo(0);
            assertThat(table.recordIndex(1)).isEqualTo(1);
            assertThat(table.recordIndex(2)).isEqualTo(2);
            assertThat(table.recordIndex(3)).isEqualTo(3);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testCopiesOneFlatBinaryGroupWithoutMaterializingAllGroups()
    {
        Vector[] values = {utf8("alpha", "beta", "gamma")};
        Vector[] nulls = {null};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                16,
                true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("testFlatBinaryGroupCopy");
        try {
            table.beginBatch(values, nulls);
            assertThat(table.assignGroup(values, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(values, nulls, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(values, nulls, 2, 2)).isEqualTo(2);
            table.endBatch();

            Streams output = table.copyGroupedValuePosition(0, null, 2, 0, 1, allocator, allocationContext);
            BinaryVector binary = (BinaryVector) output.values();
            assertThat(new String(binary.data(), binary.startOffset(0), binary.length(0), StandardCharsets.UTF_8))
                    .isEqualTo("gamma");

            output = table.copyGroupedValuePosition(0, output, 0, 0, 1, allocator, allocationContext);
            binary = (BinaryVector) output.values();
            assertThat(new String(binary.data(), binary.startOffset(0), binary.length(0), StandardCharsets.UTF_8))
                    .isEqualTo("alpha");
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testCopiedNullBinaryGroupCarriesForwardVariableWidthOffset()
    {
        Vector[] values = {utf8("alpha", "ignored")};
        org.weakref.nitro.data.BooleanVector fieldNulls = new org.weakref.nitro.data.BooleanVector(2);
        fieldNulls.values()[1] = true;
        Vector[] nulls = {fieldNulls};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                4,
                true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("testCopiedNullBinaryOffset");
        try {
            table.beginBatch(values, nulls);
            assertThat(table.assignGroup(values, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(values, nulls, 1, 1)).isEqualTo(1);
            table.endBatch();

            Streams output = table.copyGroupedValuePosition(0, null, 0, 0, 2, allocator, allocationContext);
            output = table.copyGroupedValuePosition(0, output, 1, 1, 2, allocator, allocationContext);
            BinaryVector binary = (BinaryVector) output.values();
            assertThat(binary.endOffset(1)).isEqualTo("alpha".length());

            BinaryVector copied = (BinaryVector) allocator.copyVector(allocationContext, binary);
            assertThat(new String(copied.data(), copied.startOffset(0), copied.length(0), StandardCharsets.UTF_8))
                    .isEqualTo("alpha");
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testGroupedDictionaryRangePreservesRepeatedValues()
    {
        int[] ids = {0, 1, 0, 1, 0, 1, 0, 1};
        Vector[] values = {
                DictionaryVector.wrapNested(ids, ids.length, utf8("alpha", "beta")),
                new I64Vector(new long[] {0, 1, 2, 3, 4, 5, 6, 7})};
        Vector[] nulls = {null, null};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                ids.length,
                true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("completeGroupedDictionaryDomain");
        try {
            table.beginBatch(values, nulls);
            for (int position = 0; position < ids.length; position++) {
                assertThat(table.assignGroup(values, nulls, position, position)).isEqualTo(position);
            }
            table.endBatch();

            DictionaryVector range = (DictionaryVector) table.groupedValueRangeAsDictionary(
                    0, 2, 4, Mask.all(4), allocator, allocationContext).values();
            assertThat(range.length()).isEqualTo(4);
            assertThat(range.values()).isInstanceOf(BinaryVector.class);
            assertThat(range.ids()).containsExactly(0, 1, 0, 1);
            assertThat(table.groupedValueRangeAsDictionary(
                    0, 0, ids.length, Mask.all(ids.length), allocator, allocationContext).values())
                    .isInstanceOf(DictionaryVector.class);
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testNullableGroupedValueDoesNotExposeSplitDictionaryDomain()
    {
        int[] ids = {0, 1, 0, 1};
        Vector[] values = {
                DictionaryVector.wrapNested(ids, ids.length, utf8("alpha", "beta")),
                new I64Vector(new long[] {0, 1, 2, 3})};
        org.weakref.nitro.data.BooleanVector fieldNulls = new org.weakref.nitro.data.BooleanVector(4);
        fieldNulls.values()[2] = true;
        Vector[] nulls = {fieldNulls, null};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                ids.length,
                true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("nullableGroupedDictionaryDomain");
        try {
            table.beginBatch(values, nulls);
            for (int position = 0; position < ids.length; position++) {
                assertThat(table.assignGroup(values, nulls, position, position)).isEqualTo(position);
            }
            table.endBatch();

            assertThat(table.groupedValueRangeAsDictionary(
                    0, 0, ids.length, Mask.all(ids.length), allocator, allocationContext)).isNull();
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testRejectedGroupedDictionaryOutputReleasesMappings()
    {
        Vector[] values = {
                DictionaryVector.wrapNested(new int[] {0, 1, 0, 1}, 4, utf8("alpha", "beta")),
                new I64Vector(new long[] {0, 1, 2, 3})};
        Vector[] nulls = {null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 4, true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("rejectedGroupedDictionaryOutput");
        try {
            table.beginBatch(values, nulls);
            for (int position = 0; position < 4; position++) {
                assertThat(table.assignGroup(values, nulls, position, position)).isEqualTo(position);
            }
            table.endBatch();
            assertThat(layout.tryGroupedValuesAsDictionary(table, 0, 8, Mask.all(8), allocator, context)).isNull();
            assertThat(table.groupedValueRangeAsDictionary(0, 2, 4, Mask.all(4), allocator, context)).isNull();
            long allocatedAfterWarmup = allocator.totalBytes(context);
            for (int iteration = 0; iteration < 10; iteration++) {
                assertThat(layout.tryGroupedValuesAsDictionary(table, 0, 8, Mask.all(8), allocator, context)).isNull();
                assertThat(allocator.totalBytes(context)).isEqualTo(allocatedAfterWarmup);
                assertThat(table.groupedValueRangeAsDictionary(0, 2, 4, Mask.all(4), allocator, context)).isNull();
                assertThat(allocator.totalBytes(context)).isEqualTo(allocatedAfterWarmup);
            }
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testNormalizedIntKeyPreservesCompleteEqualityAcrossBinaryEncodings()
    {
        Vector[] firstValues = {
                DictionaryVector.wrapNested(new int[] {0, 0, 1, 0}, 4, utf8("alpha", "beta")),
                new I64Vector(new long[] {1, 1, 2, 1}),
                new I64Vector(new long[] {10, 10, 20, 11})};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(firstValues, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        assertThat(layout.supportsNormalizedIntKeyShape()).isTrue();
        assertThat(layout.supportsNormalizedRecordWrite()).isTrue();
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("normalizedIntKeyOutput");
        try {
            table.beginBatch(firstValues, nulls);
            table.prepareBatchHashes(firstValues, nulls, Mask.all(4));
            assertThat(table.assignGroup(firstValues, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(firstValues, nulls, 1, 1)).isEqualTo(0);
            assertThat(table.assignGroup(firstValues, nulls, 2, 1)).isEqualTo(1);
            assertThat(table.assignGroup(firstValues, nulls, 3, 2)).isEqualTo(2);
            table.endBatch();

            // The same binary value arrives flat instead of dictionary-encoded. Query-stable interning must keep
            // its normalized key identical; the negative long declines normalization and exercises exact fallback.
            Vector[] laterValues = {
                    utf8("beta", "alpha"),
                    new I64Vector(new long[] {2, 1}),
                    new I64Vector(new long[] {20, -10})};
            table.beginBatch(laterValues, nulls);
            table.prepareBatchHashes(laterValues, nulls, Mask.all(2));
            assertThat(table.assignGroup(laterValues, nulls, 0, 3)).isEqualTo(1);
            assertThat(table.assignGroup(laterValues, nulls, 1, 3)).isEqualTo(3);
            table.endBatch();

            Streams binaryOutput = table.groupedValues(0, Mask.all(4), null, allocator, allocationContext);
            BinaryVector binaryValues = (BinaryVector) binaryOutput.values();
            assertThat(new String(
                    binaryValues.data(),
                    binaryValues.startOffset(0),
                    binaryValues.length(0),
                    StandardCharsets.UTF_8))
                    .isEqualTo("alpha");
            assertThat(new String(
                    binaryValues.data(),
                    binaryValues.startOffset(1),
                    binaryValues.length(1),
                    StandardCharsets.UTF_8))
                    .isEqualTo("beta");

            I64Vector firstLongOutput = (I64Vector) table.groupedValues(1, Mask.all(4), null, allocator, allocationContext).values();
            I64Vector secondLongOutput = (I64Vector) table.groupedValues(2, Mask.all(4), null, allocator, allocationContext).values();
            assertThat(firstLongOutput.values()).containsExactly(1, 2, 1, 1);
            assertThat(secondLongOutput.values()).containsExactly(10, 20, 11, -10);
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testSparseNormalizedBinaryGroupCopyClearsRecycledPrefixOffsets()
    {
        Vector[] values = {
                DictionaryVector.wrapNested(new int[] {0, 1}, 2, utf8("alpha", "beta")),
                new I64Vector(new long[] {1, 2}),
                new I64Vector(new long[] {10, 20})};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        assertThat(layout.supportsNormalizedRecordWrite()).isTrue();
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("sparseNormalizedBinaryGroupCopy");
        try {
            table.beginBatch(values, nulls);
            table.prepareBatchHashes(values, nulls, Mask.all(2));
            assertThat(table.assignGroup(values, nulls, 0, 0)).isZero();
            assertThat(table.assignGroup(values, nulls, 1, 1)).isEqualTo(1);
            table.endBatch();

            BinaryVector recycled = BinaryVector.allocate(allocator, allocationContext, 7, 32);
            for (int position = 0; position < recycled.length(); position++) {
                recycled.setBytes(position, new byte[] {(byte) ('a' + position)});
            }
            allocator.release(allocationContext, recycled);

            Streams output = table.copyGroupedValuePosition(0, null, 0, 1, 7, allocator, allocationContext);
            output = table.copyGroupedValuePosition(0, output, 1, 5, 7, allocator, allocationContext);
            BinaryVector binary = (BinaryVector) output.values();
            assertThat(binary).isSameAs(recycled);
            assertThat(binary.offsets()).containsExactly(0, 0, 5, 5, 5, 5, 9, 0);
            assertThat(binary.copyBytes(1)).containsExactly("alpha".getBytes(StandardCharsets.UTF_8));
            assertThat(binary.copyBytes(5)).containsExactly("beta".getBytes(StandardCharsets.UTF_8));
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testGeneratedNormalizedIntBatchHandlesSparseNullAndFallbackRows()
    {
        int size = 64;
        int[] dictionaryIds = new int[size];
        long[] first = new long[size];
        long[] second = new long[size];
        boolean[] binaryNulls = new boolean[size];
        Arrays.fill(dictionaryIds, 1);
        Arrays.fill(first, 7);
        Arrays.fill(second, 11);
        first[31] = -1;
        binaryNulls[33] = true;
        binaryNulls[35] = true;
        Vector[] values = {
                DictionaryVector.wrapNested(dictionaryIds, size, utf8("alpha", "beta")),
                new I64Vector(first),
                new I64Vector(second)};
        Vector[] nulls = {new BooleanVector(binaryNulls), null, null};
        Mask mask = Mask.sparse(new int[] {1, 3, 31, 33, 35}, size);
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, mask.selectedCount(), true);
        I64Vector groups = new I64Vector(new long[size]);
        try {
            table.beginBatch(values, nulls);
            assertThat(table.assignNormalizedIntBatch(values, nulls, mask, groups, 0)).isEqualTo(3);
            table.endBatch();

            assertThat(groups.values()[1]).isEqualTo(0);
            assertThat(groups.values()[3]).isEqualTo(0);
            assertThat(groups.values()[31]).isEqualTo(1);
            assertThat(groups.values()[33]).isEqualTo(2);
            assertThat(groups.values()[35]).isEqualTo(2);
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testAllLongNormalizedKeyCanWriteExactNormalizedRecords()
    {
        Vector[] values = {
                new I64Vector(new long[] {1, 2}),
                new I64Vector(new long[] {10, 20}),
                new I64Vector(new long[] {100, 200})};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        try {
            assertThat(layout.supportsNormalizedIntKeyShape()).isTrue();
            assertThat(layout.supportsNormalizedRecordWrite()).isTrue();
            Vector[] nulls = {new RleVector(new int[] {1, 1}, new BooleanVector(new boolean[] {false, true})), null, null};
            I64Vector groups = new I64Vector(new long[2]);
            table.beginBatch(values, nulls);
            assertThat(table.assignNormalizedIntBatch(values, nulls, Mask.all(2), groups, 0)).isEqualTo(2);
            assertThat(table.normalizedRecordValid(0)).isTrue();
            assertThat(table.normalizedRecordValid(1)).isTrue();
            assertThat(table.fieldHasNull(0)).isTrue();
            assertThat(table.fieldHasNull(1)).isFalse();
            assertThat(table.fieldHasNull(2)).isFalse();
            assertThat(table.fieldNull(1, 0)).isTrue();
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testPackedNormalizedTriplePromotesWithoutLosingExactRecords()
    {
        Vector[] firstValues = {
                new I64Vector(new long[] {1, 2}),
                new I64Vector(new long[] {10, 20}),
                new I64Vector(new long[] {100, 200})};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(firstValues, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("packedNormalizedTripleOutput");
        try {
            table.beginBatch(firstValues, nulls);
            I64Vector firstGroups = new I64Vector(2);
            assertThat(table.assignNormalizedIntBatch(firstValues, nulls, Mask.all(2), firstGroups, 0)).isEqualTo(2);
            assertThat(firstGroups.values()).containsExactly(0, 1);
            table.endBatch();
            assertThat(table.usesPackedNormalizedTripleRecords()).isTrue();

            Vector[] laterValues = {
                    new I64Vector(new long[] {1, 3_000_000}),
                    new I64Vector(new long[] {10, 30}),
                    new I64Vector(new long[] {100, 300})};
            table.beginBatch(laterValues, nulls);
            I64Vector laterGroups = new I64Vector(2);
            assertThat(table.assignNormalizedIntBatch(laterValues, nulls, Mask.all(2), laterGroups, 2)).isEqualTo(3);
            assertThat(laterGroups.values()).containsExactly(0, 2);
            table.endBatch();
            assertThat(table.usesPackedNormalizedTripleRecords()).isFalse();

            I64Vector first = (I64Vector) table.groupedValues(0, Mask.all(3), null, allocator, allocationContext).values();
            I64Vector second = (I64Vector) table.groupedValues(1, Mask.all(3), null, allocator, allocationContext).values();
            I64Vector third = (I64Vector) table.groupedValues(2, Mask.all(3), null, allocator, allocationContext).values();
            assertThat(first.values()).containsExactly(1, 2, 3_000_000);
            assertThat(second.values()).containsExactly(10, 20, 30);
            assertThat(third.values()).containsExactly(100, 200, 300);
        }
        finally {
            table.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void testNormalizedIntKeyEligibilityTransitionPreservesExistingGroups()
    {
        Vector[] firstValues = {
                DictionaryVector.wrapNested(new int[] {0, 1}, 2, utf8("alpha", "outside-domain")),
                new I64Vector(new long[] {1, -1}),
                new I64Vector(new long[] {10, 20})};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(firstValues, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
        try {
            // The negative value makes this physical batch ineligible for normalized-key scratch, even though the
            // first row itself is in-domain and may recur in a later eligible batch.
            table.beginBatch(firstValues, nulls);
            table.prepareBatchHashes(firstValues, nulls, Mask.all(2));
            assertThat(table.assignGroup(firstValues, nulls, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(firstValues, nulls, 1, 1)).isEqualTo(1);
            table.endBatch();

            Vector[] laterValues = {
                    DictionaryVector.wrapNested(new int[] {0}, 1, utf8("alpha")),
                    new I64Vector(new long[] {1}),
                    new I64Vector(new long[] {10})};
            table.beginBatch(laterValues, nulls);
            assertThat(layout.tryPrepareNormalizedIntKey(laterValues, nulls, 0)).isTrue();
            table.prepareBatchHashes(laterValues, nulls, Mask.all(1));
            assertThat(table.assignGroup(laterValues, nulls, 0, 2)).isEqualTo(0);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testFullWidthNormalizedIntKeyAdmitsExactIntDomain()
    {
        int positions = 128;
        String[] strings = new String[positions];
        long[] first = new long[positions];
        long[] second = new long[positions];
        long[] third = new long[positions];
        for (int position = 0; position < positions; position++) {
            strings[position] = "value-" + (position % 8);
            first[position] = position % 16;
            second[position] = position % 4;
            third[position] = position % 2;
        }
        Vector[] values = {utf8(strings), new I64Vector(first), new I64Vector(second), new I64Vector(third)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, new Vector[] {null, null, null, null});
            assertThat(layout.batchSupportsNormalizedIntKey()).isTrue();
            assertThat(layout.tryPrepareNormalizedIntKey(values, new Vector[] {null, null, null, null}, positions - 1)).isTrue();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testNormalizedIntKeyAdmitsMultipleReusableBinaryFieldsThatRequireInterning()
    {
        int positions = 128;
        String[] first = new String[positions];
        String[] second = new String[positions];
        long[] third = new long[positions];
        for (int position = 0; position < positions; position++) {
            first[position] = "first-" + (position % 8);
            second[position] = "second-" + (position % 16);
            third[position] = position % 32;
        }
        Vector[] values = {utf8(first), utf8(second), new I64Vector(third)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, new Vector[] {null, null, null});
            assertThat(layout.supportsNormalizedIntKeyShape()).isTrue();
            assertThat(layout.batchSupportsNormalizedIntKey()).isTrue();
            assertThat(layout.tryPrepareNormalizedIntKey(values, new Vector[] {null, null, null}, positions - 1)).isTrue();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testNormalizedIntKeyAdmitsMultipleBinaryFieldsWithStableDictionaryIds()
    {
        int positions = 128;
        int[] firstIds = new int[positions];
        int[] secondIds = new int[positions];
        long[] third = new long[positions];
        for (int position = 0; position < positions; position++) {
            firstIds[position] = position % 2;
            secondIds[position] = position % 3;
            third[position] = position % 32;
        }
        Vector[] values = {
                DictionaryVector.wrapNested(firstIds, positions, utf8("first-a", "first-b")),
                DictionaryVector.wrapNested(secondIds, positions, utf8("second-a", "second-b", "second-c")),
                new I64Vector(third)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, new Vector[] {null, null, null});
            assertThat(layout.batchSupportsNormalizedIntKey()).isTrue();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testCompactNormalizedIntKeyRejectsHighCardinalityIntegerDiscriminator()
    {
        int positions = 128;
        String[] strings = new String[positions];
        long[] first = new long[positions];
        long[] second = new long[positions];
        for (int position = 0; position < positions; position++) {
            strings[position] = "value-" + position;
            first[position] = position;
            second[position] = position + 1;
        }
        Vector[] values = {utf8(strings), new I64Vector(first), new I64Vector(second)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, new Vector[] {null, null, null});
            assertThat(layout.batchSupportsNormalizedIntKey()).isFalse();
            assertThat(layout.tryPrepareNormalizedIntKey(values, new Vector[] {null, null, null}, 0)).isFalse();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testNormalizedIntKeyAdmitsHighCardinalityIntegerWithOneReusableBinaryField()
    {
        int positions = 128;
        String[] strings = new String[positions];
        long[] first = new long[positions];
        long[] second = new long[positions];
        for (int position = 0; position < positions; position++) {
            strings[position] = "value-" + (position % 8);
            first[position] = position;
            second[position] = position % 4;
        }
        Vector[] values = {utf8(strings), new I64Vector(first), new I64Vector(second)};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, new Vector[] {null, null, null});
            assertThat(layout.batchSupportsNormalizedIntKey()).isTrue();
            assertThat(layout.tryPrepareNormalizedIntKey(values, new Vector[] {null, null, null}, 0)).isTrue();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testGeneratedHybridHashHandlesRejectedNormalizedVariableWidthBatch()
    {
        int positions = 128;
        String[] strings = new String[positions];
        long[] first = new long[positions];
        long[] second = new long[positions];
        for (int position = 0; position < positions; position++) {
            strings[position] = "value-" + position;
            first[position] = position;
            second[position] = position + 1;
        }
        Vector[] values = {utf8(strings), new I64Vector(first), new I64Vector(second)};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, positions, true);
        try {
            table.beginBatch(values, nulls);
            assertThat(layout.supportsNormalizedIntKeyShape()).isTrue();
            assertThat(layout.batchSupportsNormalizedIntKey()).isFalse();
            I64Vector groups = new I64Vector(positions);
            assertThat(table.assignGeneratedDictionaryBatch(
                    values, nulls, Mask.all(positions), groups, 0)).isEqualTo(positions);
            assertThat(groups.values()).containsExactly(first);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testRejectedNormalizedRecordsAllowPackedIdentitySlots()
    {
        int positions = 128;
        String[] strings = new String[positions];
        long[] first = new long[positions];
        long[] second = new long[positions];
        for (int position = 0; position < positions; position++) {
            strings[position] = "value-" + position;
            first[position] = position;
            second[position] = position + 1;
        }
        Vector[] values = {utf8(strings), new I64Vector(first), new I64Vector(second)};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        layout.beginBatch(values, nulls);
        assertThat(layout.batchSupportsNormalizedIntKey()).isFalse();
        layout.endBatch();

        FlatGroupingTable table = new FlatGroupingTable(layout, positions, true, true);
        try {
            assertThat(table.usesPackedHashRecordSlots()).isTrue();
            table.beginBatch(values, nulls);
            I64Vector groups = new I64Vector(positions);
            assertThat(table.assignGeneratedDictionaryBatch(
                    values, nulls, Mask.all(positions), groups, 0)).isEqualTo(positions);
            assertThat(groups.values()).containsExactly(first);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testCompactNormalizedIntKeyRejectsMostlyOutOfDomainIntegerLane()
    {
        int positions = 128;
        String[] strings = new String[positions];
        long[] userIds = new long[positions];
        long[] categories = new long[positions];
        for (int position = 0; position < positions; position++) {
            strings[position] = "value-" + (position % 8);
            userIds[position] = (1L << 40) + position;
            categories[position] = position % 4;
        }
        Vector[] values = {utf8(strings), new I64Vector(userIds), new I64Vector(categories)};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, nulls);
            assertThat(layout.batchSupportsNormalizedIntKey()).isFalse();
            assertThat(layout.tryPrepareNormalizedIntKey(values, nulls, 0)).isFalse();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testNormalizedIntKeyDeclinesConstantLongLane()
    {
        int positions = 128;
        String[] strings = new String[positions];
        long[] first = new long[positions];
        for (int position = 0; position < positions; position++) {
            strings[position] = "value-" + position;
            first[position] = position;
        }
        Vector[] values = {
                utf8(strings),
                new I64Vector(first),
                new RleVector(new int[] {positions}, new I64Vector(new long[] {7}))};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        try {
            layout.beginBatch(values, new Vector[] {null, null, null});
            assertThat(layout.supportsNormalizedIntKeyShape()).isTrue();
            assertThat(layout.batchSupportsNormalizedIntKey()).isFalse();
            layout.endBatch();
        }
        finally {
            layout.releaseBuffers();
        }
    }

    @Test
    void testOutOfRangeMixedLongRejectsBeforeEagerDictionaryInterning()
    {
        int dictionarySize = 2_048;
        String[] firstDictionary = new String[dictionarySize];
        String[] secondDictionary = new String[dictionarySize];
        for (int index = 0; index < dictionarySize; index++) {
            firstDictionary[index] = "first-" + index;
            secondDictionary[index] = "second-" + index;
        }
        Vector[] values = {
                DictionaryVector.wrapNested(new int[] {0}, 1, utf8(firstDictionary)),
                DictionaryVector.wrapNested(new int[] {0}, 1, utf8(secondDictionary)),
                new I64Vector(new long[] {10_000})};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 1, true);
        try {
            table.beginBatch(values, nulls);
            assertThat(table.batchArrayModeEligible()).isFalse();
            assertThat(layout.internedValueCount(0)).isZero();
            assertThat(layout.internedValueCount(1)).isZero();

            assertThat(table.assignGroup(values, nulls, 0, 0)).isZero();
            assertThat(layout.internedValueCount(0)).isEqualTo(1);
            assertThat(layout.internedValueCount(1)).isEqualTo(1);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testSparseMixedCompositeDictionaryInternsOnlyReferencedEntries()
    {
        int dictionarySize = 2_048;
        String[] firstDictionary = new String[dictionarySize];
        String[] secondDictionary = new String[dictionarySize];
        for (int index = 0; index < dictionarySize; index++) {
            firstDictionary[index] = "first-" + index;
            secondDictionary[index] = "second-" + index;
        }
        Vector[] values = {
                DictionaryVector.wrapNested(new int[] {17}, 1, utf8(firstDictionary)),
                DictionaryVector.wrapNested(new int[] {29}, 1, utf8(secondDictionary)),
                new I64Vector(new long[] {1})};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 1, true);
        try {
            table.beginBatch(values, nulls);
            assertThat(layout.internedValueCount(0)).isZero();
            assertThat(layout.internedValueCount(1)).isZero();

            assertThat(table.assignGroup(values, nulls, 0, 0)).isZero();
            assertThat(layout.internedValueCount(0)).isEqualTo(1);
            assertThat(layout.internedValueCount(1)).isEqualTo(1);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testDenseMixedCompositeFieldDoesNotForceSparseFieldInterning()
    {
        int positions = 2_048;
        String[] sparseDictionary = new String[positions * 2];
        String[] denseDictionary = new String[positions];
        int[] sparseIds = new int[positions];
        int[] denseIds = new int[positions];
        long[] longValues = new long[positions];
        Arrays.fill(sparseIds, 17);
        Arrays.fill(longValues, 1);
        for (int index = 0; index < sparseDictionary.length; index++) {
            sparseDictionary[index] = "sparse-" + index;
        }
        for (int index = 0; index < denseDictionary.length; index++) {
            denseDictionary[index] = "dense-" + index;
            denseIds[index] = index;
        }
        Vector[] values = {
                DictionaryVector.wrapNested(sparseIds, positions, utf8(sparseDictionary)),
                DictionaryVector.wrapNested(denseIds, positions, utf8(denseDictionary)),
                new I64Vector(longValues)};
        Vector[] nulls = {null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 1, true);
        try {
            table.beginBatch(values, nulls);
            assertThat(layout.internedValueCount(0)).isZero();
            assertThat(layout.internedValueCount(1)).isEqualTo(positions);

            assertThat(table.assignGroup(values, nulls, 0, 0)).isZero();
            assertThat(layout.internedValueCount(0)).isEqualTo(1);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testSuppliedBatchHashesRetainExactCollisionChecks()
    {
        Vector[] values = {utf8("alpha", "beta", "alpha")};
        Vector[] nulls = {null};
        Mask mask = Mask.all(3);
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                3,
                true);
        try {
            table.beginBatch(values, nulls, mask);
            // Deliberately collide every logical key. The supplied hash is authoritative for probe placement, but
            // the complete key remains authoritative for equality.
            table.prepareAuthoritativeBatchHashes(new I64Vector(new long[] {11, 11, 11}), mask);
            long nextGroupId = 0;
            long[] groups = new long[3];
            for (int position = 0; position < groups.length; position++) {
                groups[position] = table.assignGroup(values, nulls, position, nextGroupId);
                if (groups[position] == nextGroupId) {
                    nextGroupId++;
                }
            }

            assertThat(groups).containsExactly(0, 1, 0);
            assertThat(nextGroupId).isEqualTo(2);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testSuppliedBatchHashesMustCoverMaskAddressSpace()
    {
        Vector[] values = {utf8("alpha", "beta")};
        Vector[] nulls = {null};
        FlatGroupingTable table = new FlatGroupingTable(
                FlatKeyLayout.tryCreate(values, true, arrayPool, codeGeneration, flatKeyTablePolicy),
                2,
                true);
        try {
            Mask mask = Mask.all(2);
            table.beginBatch(values, nulls, mask);
            assertThatThrownBy(() -> table.prepareAuthoritativeBatchHashes(new I64Vector(1), mask))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Hash vector has 1 positions, but mask requires 2");
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testGroupingStateConsumesSuppliedFlatHashes()
    {
        Vector[] values = {utf8("alpha", "beta", "alpha")};
        Vector[] nulls = {null};
        I64Vector groups = new I64Vector(3);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("supplied-grouping-hashes");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            assertThat(state.assignGroupsWithAuthoritativeHashes(
                    values,
                    nulls,
                    Mask.all(3),
                    groups,
                    new I64Vector(new long[] {23, 23, 23})))
                    .isTrue();
            assertThat(groups.values()).containsExactly(0, 1, 0);
            assertThat(state.groupCount()).isEqualTo(2);
            assertThat(state.groupedHashRange(0, 2, null, allocator, context).values())
                    .containsExactly(23, 23);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGroupingStateConsumesRegionOfSuppliedFlatHashes()
    {
        Vector[] values = {utf8("alpha", "beta", "alpha")};
        Vector[] nulls = {null};
        I64Vector groups = new I64Vector(3);
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            assertThat(state.assignGroupsWithAuthoritativeHashes(
                    values,
                    nulls,
                    Mask.all(3),
                    groups,
                    new RegionVector(new I64Vector(new long[] {99, 23, 23, 23}), 1, 3)))
                    .isTrue();
            assertThat(groups.values()).containsExactly(0, 1, 0);
            assertThat(state.groupCount()).isEqualTo(2);
        }
        finally {
            state.releaseBuffers();
        }
    }

    @Test
    void testGroupingStateDoesNotExportInternalCompositeProbeHashes()
    {
        Vector[] values = {utf8("alpha", "beta"), new I64Vector(new long[] {11, 22})};
        Vector[] nulls = {null, null};
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("internal-grouping-hashes");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            state.assignGroups(values, nulls, Mask.all(2), new I64Vector(2));
            assertThat(state.groupedHashRange(0, 2, null, allocator, context)).isNull();
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGroupingStateRetainsSuppliedHashesInSpecializedLongGrouping()
    {
        Vector[] values = {new I64Vector(new long[] {1, 2, 1, 3, 2})};
        Vector[] nulls = {null};
        I64Vector groupIds = new I64Vector(5);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("authoritative-long-hashes");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            assertThat(state.assignGroupsWithAuthoritativeHashes(
                    values,
                    nulls,
                    Mask.all(5),
                    groupIds,
                    // The first two keys deliberately collide. Complete-key equality remains authoritative.
                    new I64Vector(new long[] {31, 31, 31, 43, 31})))
                    .isTrue();
            assertThat(state.usesSingleLongGrouping()).isTrue();
            assertThat(groupIds.values()).containsExactly(0, 1, 0, 2, 1);
            assertThat(state.groupCount()).isEqualTo(3);
            assertThat(state.groupedHashRange(0, 3, null, allocator, context).values())
                    .containsExactly(31, 31, 43);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testSpecializedLongGroupingRetainsAuthoritativeNullHash()
    {
        Vector[] values = {new I64Vector(new long[] {11, 0, 22, 0})};
        Vector[] nulls = {new BooleanVector(new boolean[] {false, true, false, true})};
        I64Vector groupIds = new I64Vector(4);
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("authoritative-null-hash");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            assertThat(state.assignGroupsWithAuthoritativeHashes(
                    values,
                    nulls,
                    Mask.all(4),
                    groupIds,
                    new I64Vector(new long[] {101, 7, 202, 7})))
                    .isTrue();
            assertThat(groupIds.values()).containsExactly(0, 1, 2, 1);
            assertThat(state.groupedHashRange(0, 3, null, allocator, context).values())
                    .containsExactly(101, 7, 202);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testSpecializedLongGroupingRehashesWithAuthoritativeHashes()
    {
        long[] keys = new long[48];
        long[] hashes = new long[48];
        for (int index = 0; index < keys.length; index++) {
            keys[index] = index;
            hashes[index] = index % 3;
        }
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("authoritative-long-rehash");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            I64Vector groupIds = new I64Vector(keys.length);
            assertThat(state.assignGroupsWithAuthoritativeHashes(
                    new Vector[] {new I64Vector(keys)},
                    new Vector[] {null},
                    Mask.all(keys.length),
                    groupIds,
                    new I64Vector(hashes)))
                    .isTrue();
            assertThat(groupIds.values()).containsExactly(keys);

            I64Vector repeatedGroups = new I64Vector(3);
            assertThat(state.assignGroupsWithAuthoritativeHashes(
                    new Vector[] {new I64Vector(new long[] {47, 0, 23})},
                    new Vector[] {null},
                    Mask.all(3),
                    repeatedGroups,
                    new I64Vector(new long[] {2, 0, 2})))
                    .isTrue();
            assertThat(repeatedGroups.values()).containsExactly(47, 0, 23);
            assertThat(state.groupedHashRange(0, keys.length, null, allocator, context).values())
                    .containsExactly(hashes);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testSpecializedLongGroupingConsumesAlignedAuthoritativeDictionaryDomain()
    {
        int[] ids = {2, 0, 2, 1, 0, 2};
        DictionaryVector keys = DictionaryVector.ofTrustedIds(ids, new I64Vector(new long[] {11, 22, 33}));
        DictionaryVector hashes = keys.sharedMappingWithValues(new I64Vector(new long[] {101, 202, 303}));
        int[] counts = new int[4];
        int[] groups = new int[4];
        int[] representatives = new int[4];
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("authoritative-long-dictionary-domain");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            state.initializeSchemaWithAuthoritativeHashes(
                    new Vector[] {keys},
                    new Vector[] {null},
                    Mask.all(ids.length));
            assertThat(state.assignSingleDictionaryDomainWithAuthoritativeHashes(
                    keys,
                    null,
                    Mask.all(ids.length),
                    counts,
                    groups,
                    representatives,
                    hashes))
                    .isEqualTo(4);
            assertThat(counts).containsExactly(2, 1, 3, 0);
            assertThat(groups).startsWith(0, 1, 2);
            assertThat(state.groupedHashRange(0, 3, null, allocator, context).values())
                    .containsExactly(101, 202, 303);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testFlatGroupingConsumesAlignedAuthoritativeDictionaryDomain()
    {
        int[] ids = {1, 0, 1, 2, 0, 1};
        DictionaryVector keys = DictionaryVector.ofTrustedIds(ids, utf8("alpha", "beta", "gamma"));
        DictionaryVector hashes = keys.sharedMappingWithValues(new I64Vector(new long[] {17, 17, 29}));
        int[] counts = new int[4];
        int[] groups = new int[4];
        int[] representatives = new int[4];
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("authoritative-flat-dictionary-domain");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            state.initializeSchemaWithAuthoritativeHashes(
                    new Vector[] {keys},
                    new Vector[] {null},
                    Mask.all(ids.length));
            assertThat(state.assignSingleDictionaryDomainWithAuthoritativeHashes(
                    keys,
                    null,
                    Mask.all(ids.length),
                    counts,
                    groups,
                    representatives,
                    hashes))
                    .isEqualTo(4);
            assertThat(counts).containsExactly(2, 3, 1, 0);
            assertThat(groups).startsWith(0, 1, 2);
            assertThat(state.groupedHashRange(0, 3, null, allocator, context).values())
                    .containsExactly(17, 17, 29);
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testGroupingStateDoesNotExportIncompleteHashesForExternalNullGroup()
    {
        Vector[] values = {utf8("ignored", "alpha")};
        Vector[] nulls = {new BooleanVector(new boolean[] {true, false})};
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context context = new Allocator.Context("external-null-group-hashes");
        GroupingState state = new GroupingState(
                arrayPool,
                codeGeneration,
                groupingResources,
                adaptiveLongGroupingPolicy,
                flatKeyTablePolicy);
        try {
            state.assignGroups(values, nulls, Mask.all(2), new I64Vector(2));
            assertThat(state.groupedHashRange(0, 2, null, allocator, context)).isNull();
        }
        finally {
            state.releaseBuffers();
            allocator.release(context);
        }
    }

    private static BinaryVector utf8(String... values)
    {
        int bytes = 0;
        for (String value : values) {
            bytes += value.getBytes(StandardCharsets.UTF_8).length;
        }
        BinaryVector vector = new BinaryVector(values.length, bytes);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, values[index].getBytes(StandardCharsets.UTF_8));
        }
        return vector;
    }

    private static MapVector maps(long[][] keys, String[][] values, boolean[][] valueNulls)
    {
        assertThat(values.length).isEqualTo(keys.length);
        assertThat(valueNulls.length).isEqualTo(keys.length);
        int entryCount = Arrays.stream(keys).mapToInt(row -> row.length).sum();
        long[] flatKeys = new long[entryCount];
        String[] flatValues = new String[entryCount];
        boolean[] flatNulls = new boolean[entryCount];
        MapVector result = new MapVector(keys.length);
        int offset = 0;
        for (int row = 0; row < keys.length; row++) {
            assertThat(values[row]).hasSize(keys[row].length);
            assertThat(valueNulls[row]).hasSize(keys[row].length);
            result.offsets()[row] = offset;
            System.arraycopy(keys[row], 0, flatKeys, offset, keys[row].length);
            System.arraycopy(values[row], 0, flatValues, offset, values[row].length);
            System.arraycopy(valueNulls[row], 0, flatNulls, offset, valueNulls[row].length);
            offset += keys[row].length;
        }
        result.offsets()[keys.length] = offset;
        result.setEntries(
                Streams.ofValues(new I64Vector(flatKeys)),
                Streams.builder()
                        .put(org.weakref.nitro.data.Stream.VALUES, utf8(flatValues))
                        .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(flatNulls))
                        .build());
        return result;
    }

    private static TypeBinding longBinding(String identity, Optional<LongFlatKeyStorage> flatKeyStorage)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity(identity);
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
            public Optional<LongFlatKeyStorage> longFlatKeyStorage()
            {
                return flatKeyStorage;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I32Vector.class, I64Vector.class, DictionaryVector.class);
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
                return new TypeIdentity("testing:raw-binary-key");
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
                return Set.of(BinaryVector.class, DictionaryVector.class, RegionVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding rawLongType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:raw-long-key");
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
            public boolean supportsRawKeyIdentity()
            {
                return true;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I32Vector.class, I64Vector.class, DictionaryVector.class, RegionVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding rawRepeatedElementType(
            String identity,
            Class<?> carrierType,
            Set<Class<? extends Vector>> vectorTypes)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity(identity);
            }

            @Override
            public Class<?> carrierType()
            {
                return carrierType;
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
                return vectorTypes;
            }
        };
    }

    private static TypeBinding orderedRepeatedType(TypeBinding elements)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:ordered-repeated-key");
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
                return List.of(elements);
            }

            @Override
            public Optional<RepeatedKeyLayout> repeatedKeyLayout()
            {
                return Optional.of(new RepeatedKeyLayout(
                        RepeatedKeyLayout.Order.ORDERED,
                        List.of(new RepeatedKeyLayout.Output(0, elements))));
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(ArrayVector.class, DictionaryVector.class, RegionVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding unorderedRepeatedMapType()
    {
        TypeBinding keys = rawLongType();
        TypeBinding values = rawBinaryType();
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:unordered-repeated-map-key");
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
                return List.of(keys, values);
            }

            @Override
            public Optional<RepeatedKeyLayout> repeatedKeyLayout()
            {
                return Optional.of(new RepeatedKeyLayout(
                        RepeatedKeyLayout.Order.UNORDERED_MULTISET,
                        List.of(
                                new RepeatedKeyLayout.Output(0, keys),
                                new RepeatedKeyLayout.Output(1, values))));
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(MapVector.class, DictionaryVector.class, RegionVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding fixedWidthPairType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:fixed-width-product-pair");
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
                return Set.of(StructVector.class, DictionaryVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding productType(TypeBinding timestamp, TypeBinding label)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:recursive-product-key");
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
                return List.of(timestamp, label);
            }

            @Override
            public Optional<PersistentKeyLayout> persistentKeyLayout()
            {
                return Optional.of(new PersistentKeyLayout(List.of(
                        new PersistentKeyLayout.Field(List.of("timestamp"), timestamp),
                        new PersistentKeyLayout.Field(List.of("label"), label))));
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class, DictionaryVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding canonicalDoubleType()
    {
        FixedWidthKeyLayout layout = new FixedWidthKeyLayout(List.of(FixedWidthKeyLayout.Lane.projected(
                List.of(new FixedWidthKeyLayout.Source(List.of(), FixedWidthKeyLayout.Carrier.F64)),
                CANONICAL_DOUBLE)));
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:canonical-double-key");
            }

            @Override
            public Class<?> carrierType()
            {
                return double.class;
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

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(F64Vector.class, DictionaryVector.class, RegionVector.class, RleVector.class);
            }
        };
    }

    private static MethodHandle canonicalDoubleHandle()
    {
        try {
            return MethodHandles.lookup().findStatic(
                    TestFlatGroupingTable.class,
                    "canonicalDouble",
                    MethodType.methodType(long.class, double.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static long canonicalDouble(double value)
    {
        return Double.doubleToLongBits(value == 0.0 ? 0.0 : value);
    }

    private record LongPair(long first, int second) {}

    private record FourFieldKey(int string, int integer, int hour, int minute) {}
}
