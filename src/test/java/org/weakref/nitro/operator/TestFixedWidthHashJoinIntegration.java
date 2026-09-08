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
import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.core.type.PersistentKeyLayout;
import org.weakref.nitro.core.type.RepeatedKeyLayout;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFixedWidthHashJoinIntegration
{
    @Test
    void testSemanticHashJoinKeyIsRejectedWithoutPhysicalLayout()
            throws ReflectiveOperationException
    {
        TypeBinding pairType = semanticPairType();
        Schema schema = new Schema(List.of(new Field(pairType, false)));
        StructVector buildKeys = pairs(new long[] {1}, new long[] {10});
        TableOperator build = new TableOperator(
                schema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        schema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            StructVector probeKeys = pairs(new long[] {1}, new long[] {10});
            session.addInput(new Batch(Mask.all(1), Output.of(Streams.ofValues(probeKeys))));
            assertThatThrownBy(session::hasOutput)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("hash join")
                    .hasMessageContaining("direct physical or provider-described generated persistent key layout")
                    .hasMessageContaining("ADR-0090");
        }
    }

    @Test
    void testProviderLayoutDrivesHashJoinBuildAndProbe()
    {
        TypeBinding pairType = pairType();
        Schema probeSchema = new Schema(List.of(new Field(pairType, false)));
        Schema buildSchema = new Schema(List.of(
                new Field(pairType, false),
                Schema.unspecified(1).field(0)));
        StructVector buildKeys = pairs(
                new long[] {1, 1, 2},
                new long[] {10, 10, 20});
        TableOperator build = new TableOperator(
                buildSchema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys, new I64Vector(new long[] {100, 101, 200})},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        probeSchema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            StructVector probeKeys = pairs(
                    new long[] {1, 2, 3},
                    new long[] {10, 20, 30});
            session.addInput(new Batch(
                    Mask.all(probeKeys.length()),
                    Output.of(Streams.ofValues(DictionaryVector.wrap(
                            new int[] {0, 1, 2},
                            probeKeys)))));

            List<Long> payloads = new ArrayList<>();
            while (session.hasOutput()) {
                try (Batch output = session.getOutput()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(output.output(2).borrow(Stream.VALUES));
                    for (int position : output.borrowMask()) {
                        payloads.add(values.value(position));
                    }
                }
            }
            assertThat(payloads).containsExactly(100L, 101L, 200L);
        }
    }

    @Test
    void testCanonicalProjectionDrivesEncodedHashJoinBuildAndProbe()
    {
        TypeBinding type = CanonicalFixedWidthKeyTestType.INSTANCE;
        Schema probeSchema = new Schema(List.of(new Field(type, false)));
        Schema buildSchema = new Schema(List.of(
                new Field(type, false),
                Schema.unspecified(1).field(0)));
        I64Vector buildDomain = new I64Vector(new long[] {
                CanonicalFixedWidthKeyTestType.pack(100, 1),
                CanonicalFixedWidthKeyTestType.pack(200, 2),
                CanonicalFixedWidthKeyTestType.pack(100, 3)});
        Vector buildKeys = DictionaryVector.wrap(new int[] {0, 1, 2}, buildDomain);
        TableOperator build = new TableOperator(
                buildSchema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys, new I64Vector(new long[] {100, 200, 101})},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        probeSchema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            Vector probeKeys = new RegionVector(
                    new I64Vector(new long[] {
                            -1,
                            CanonicalFixedWidthKeyTestType.pack(100, 9),
                            CanonicalFixedWidthKeyTestType.pack(200, 8),
                            CanonicalFixedWidthKeyTestType.pack(300, 7)}),
                    1,
                    3);
            session.addInput(new Batch(
                    Mask.all(probeKeys.length()),
                    Output.of(Streams.ofValues(probeKeys))));

            List<Long> payloads = new ArrayList<>();
            while (session.hasOutput()) {
                try (Batch output = session.getOutput()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(output.output(2).borrow(Stream.VALUES));
                    for (int position : output.borrowMask()) {
                        payloads.add(values.value(position));
                    }
                }
            }
            assertThat(payloads).containsExactly(100L, 101L, 200L);
        }
    }

    @Test
    void testRecursiveProductLayoutDrivesGeneratedHashJoinBuildAndProbe()
    {
        TypeBinding pairType = pairType();
        TypeBinding labelType = rawBinaryType();
        TypeBinding productType = productType(pairType, labelType);
        Schema probeSchema = new Schema(List.of(new Field(productType, false)));
        Schema buildSchema = new Schema(List.of(
                new Field(productType, false),
                Schema.unspecified(1).field(0)));
        StructVector buildKeys = products(
                new long[] {1, 1, 2, 9},
                new long[] {10, 10, 20, 90},
                new String[] {"a", "a", "b", "ignored"},
                new boolean[] {false, false, false, true},
                new boolean[] {false, false, false, true});
        TableOperator build = new TableOperator(
                buildSchema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys, new I64Vector(new long[] {100, 101, 200, 999})},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        probeSchema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            StructVector probeDomain = products(
                    new long[] {1, 2, 3, 8},
                    new long[] {10, 20, 30, 80},
                    new String[] {"a", "b", "c", "ignored"},
                    new boolean[] {false, false, false, true},
                    new boolean[] {false, false, false, true});
            Vector probeKeys = DictionaryVector.wrap(
                    new int[] {1, 0, 3, 2, 1, 0, 3, 2, 1, 0, 3, 2, 1, 0, 3, 2},
                    probeDomain);
            session.addInput(new Batch(
                    Mask.all(probeKeys.length()),
                    Output.of(Streams.ofValues(probeKeys))));

            List<Long> payloads = new ArrayList<>();
            while (session.hasOutput()) {
                try (Batch output = session.getOutput()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(output.output(2).borrow(Stream.VALUES));
                    for (int position : output.borrowMask()) {
                        payloads.add(values.value(position));
                    }
                }
            }
            assertThat(payloads).containsExactly(
                    200L, 100L, 101L,
                    200L, 100L, 101L,
                    200L, 100L, 101L,
                    200L, 100L, 101L);
        }
    }

    @Test
    void testOrderedRepeatedLayoutDrivesHashJoinBuildAndDictionaryProbe()
    {
        TypeBinding arrayType = orderedRepeatedType(rawLongType());
        Schema probeSchema = new Schema(List.of(new Field(arrayType, false)));
        Schema buildSchema = new Schema(List.of(new Field(arrayType, false), Schema.unspecified(1).field(0)));
        ArrayVector buildKeys = arrays(new long[][] {{1, 2}, {1, 2}, {2, 1}, {}});
        TableOperator build = new TableOperator(
                buildSchema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys, new I64Vector(new long[] {100, 101, 200, 300})},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        probeSchema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            ArrayVector domain = arrays(new long[][] {{2, 1}, {1, 2}, {}, {9}});
            Vector probeKeys = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 0, 1, 2, 3}, domain);
            session.addInput(new Batch(Mask.all(probeKeys.length()), Output.of(Streams.ofValues(probeKeys))));

            List<Long> payloads = new ArrayList<>();
            while (session.hasOutput()) {
                try (Batch output = session.getOutput()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(output.output(2).borrow(Stream.VALUES));
                    for (int position : output.borrowMask()) {
                        payloads.add(values.value(position));
                    }
                }
            }
            assertThat(payloads).containsExactly(
                    200L, 100L, 101L, 300L,
                    200L, 100L, 101L, 300L);
        }
    }

    @Test
    void testOrderedRepeatedProductLayoutDrivesHashJoin()
    {
        TypeBinding rowType = productType(rawLongType(), rawBinaryType());
        TypeBinding arrayType = orderedRepeatedType(rowType);
        Schema probeSchema = new Schema(List.of(new Field(arrayType, false)));
        Schema buildSchema = new Schema(List.of(new Field(arrayType, false), Schema.unspecified(1).field(0)));
        ArrayVector buildKeys = productArrays(
                new long[] {1, 1, 2, 0},
                new String[] {"a", "a", "b", "ignored"},
                new boolean[] {false, false, false, true});
        TableOperator build = new TableOperator(
                buildSchema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys, new I64Vector(new long[] {100, 101, 200, 300})},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        probeSchema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            ArrayVector probeKeys = productArrays(
                    new long[] {2, 1, 1, 0},
                    new String[] {"b", "a", "x", "ignored"},
                    new boolean[] {false, false, false, true});
            session.addInput(new Batch(Mask.all(probeKeys.length()), Output.of(Streams.ofValues(probeKeys))));

            List<Long> payloads = new ArrayList<>();
            while (session.hasOutput()) {
                try (Batch output = session.getOutput()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(output.output(2).borrow(Stream.VALUES));
                    for (int position : output.borrowMask()) {
                        payloads.add(values.value(position));
                    }
                }
            }
            assertThat(payloads).containsExactly(200L, 100L, 101L, 300L);
        }
    }

    @Test
    void testUnorderedRepeatedLayoutDrivesHashJoinIndependentOfEntryOrder()
    {
        TypeBinding mapType = unorderedRepeatedMapType();
        Schema probeSchema = new Schema(List.of(new Field(mapType, false)));
        Schema buildSchema = new Schema(List.of(new Field(mapType, false), Schema.unspecified(1).field(0)));
        MapVector buildKeys = maps(
                new long[][] {{1, 2}, {2, 1}, {}, {3}, {1, 1}},
                new String[][] {{"a", "b"}, {"b", "a"}, {}, {"ignored"}, {"a", "a"}},
                new boolean[][] {{false, false}, {false, false}, {}, {true}, {false, false}});
        TableOperator build = new TableOperator(
                buildSchema,
                List.of(TableOperator.Page.values(
                        buildKeys.length(),
                        new Vector[] {buildKeys, new I64Vector(new long[] {100, 101, 200, 300, 400})},
                        Mask.all(buildKeys.length()))));

        try (EngineResources resources = EngineResources.createDefault();
                org.weakref.nitro.data.Allocator allocator = new org.weakref.nitro.data.Allocator(resources);
                HashJoinSession session = new HashJoinSession(
                        resources.operatorResources(),
                        allocator,
                        probeSchema,
                        new int[] {0},
                        build,
                        new int[] {0},
                        false)) {
            allocator.beginExecution();
            MapVector domain = maps(
                    new long[][] {{2, 1}, {}, {3}, {1}, {1, 1}, {1, 2}},
                    new String[][] {{"b", "a"}, {}, {"different-ignored"}, {"a"}, {"a", "a"}, {"a", "different"}},
                    new boolean[][] {{false, false}, {}, {true}, {false}, {false, false}, {false, false}});
            Vector probeKeys = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 4, 5, 0}, domain);
            session.addInput(new Batch(Mask.all(probeKeys.length()), Output.of(Streams.ofValues(probeKeys))));

            List<Long> payloads = new ArrayList<>();
            while (session.hasOutput()) {
                try (Batch output = session.getOutput()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(output.output(2).borrow(Stream.VALUES));
                    for (int position : output.borrowMask()) {
                        payloads.add(values.value(position));
                    }
                }
            }
            assertThat(payloads).containsExactly(100L, 101L, 200L, 300L, 400L, 100L, 101L);
        }
    }

    private static ArrayVector arrays(long[][] rows)
    {
        int elements = 0;
        for (long[] row : rows) {
            elements += row.length;
        }
        ArrayVector result = new ArrayVector(rows.length);
        long[] values = new long[elements];
        int offset = 0;
        for (int row = 0; row < rows.length; row++) {
            result.offsets()[row] = offset;
            System.arraycopy(rows[row], 0, values, offset, rows[row].length);
            offset += rows[row].length;
        }
        result.offsets()[rows.length] = offset;
        result.setElements(Streams.ofValues(new I64Vector(values)));
        return result;
    }

    private static ArrayVector productArrays(long[] ids, String[] labels, boolean[] nullRows)
    {
        assertThat(labels.length).isEqualTo(ids.length);
        assertThat(nullRows.length).isEqualTo(ids.length);
        StructVector rows = new StructVector(ids.length);
        rows.setField("pair", Streams.ofValues(new I64Vector(ids)));
        rows.setField("label", Streams.ofValues(utf8(labels)));
        ArrayVector result = new ArrayVector(ids.length);
        for (int position = 0; position <= ids.length; position++) {
            result.offsets()[position] = position;
        }
        result.setElements(Streams.builder()
                .put(Stream.VALUES, rows)
                .put(Stream.NULLS, new BooleanVector(nullRows))
                .build());
        return result;
    }

    private static MapVector maps(long[][] keys, String[][] values, boolean[][] valueNulls)
    {
        assertThat(values.length).isEqualTo(keys.length);
        assertThat(valueNulls.length).isEqualTo(keys.length);
        int entryCount = java.util.Arrays.stream(keys).mapToInt(row -> row.length).sum();
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
                        .put(Stream.VALUES, utf8(flatValues))
                        .put(Stream.NULLS, new BooleanVector(flatNulls))
                        .build());
        return result;
    }

    private static StructVector pairs(long[] high, long[] low)
    {
        StructVector pairs = new StructVector(high.length);
        pairs.setField("high", Streams.ofValues(new I64Vector(high)));
        pairs.setField("low", Streams.ofValues(new I64Vector(low)));
        return pairs;
    }

    private static StructVector products(
            long[] high,
            long[] low,
            String[] labels,
            boolean[] pairNulls,
            boolean[] labelNulls)
    {
        StructVector products = new StructVector(high.length);
        products.setField("pair", Streams.builder()
                .put(Stream.VALUES, pairs(high, low))
                .put(Stream.NULLS, new BooleanVector(pairNulls))
                .build());
        products.setField("label", Streams.builder()
                .put(Stream.VALUES, utf8(labels))
                .put(Stream.NULLS, new BooleanVector(labelNulls))
                .build());
        return products;
    }

    private static BinaryVector utf8(String[] values)
    {
        byte[][] encoded = java.util.Arrays.stream(values)
                .map(value -> value.getBytes(StandardCharsets.UTF_8))
                .toArray(byte[][]::new);
        BinaryVector vector = new BinaryVector(values.length, java.util.Arrays.stream(encoded).mapToInt(value -> value.length).sum());
        for (int position = 0; position < encoded.length; position++) {
            vector.setBytes(position, encoded[position]);
        }
        return vector;
    }

    private static TypeBinding pairType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:fixed-width-hash-join-pair");
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

    private static TypeBinding rawBinaryType()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:raw-binary-hash-join-key");
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
                return Set.of(BinaryVector.class, DictionaryVector.class);
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
                return new TypeIdentity("testing:raw-long-hash-join-key");
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
                return Set.of(I64Vector.class, DictionaryVector.class, RegionVector.class);
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
                return new TypeIdentity("testing:ordered-repeated-hash-join-key");
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
                return Set.of(ArrayVector.class, DictionaryVector.class, RegionVector.class);
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
                return new TypeIdentity("testing:unordered-repeated-hash-join-key");
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
                return Set.of(MapVector.class, DictionaryVector.class, RegionVector.class);
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
                return new TypeIdentity("testing:recursive-product-hash-join-key");
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
                return Set.of(StructVector.class, DictionaryVector.class);
            }
        };
    }

    private static TypeBinding semanticPairType()
            throws ReflectiveOperationException
    {
        TypeOperators operators = new TypeOperators(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(MethodHandles.lookup().findStatic(
                        TestFixedWidthHashJoinIntegration.class,
                        "unexpectedPairIdentical",
                        MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class))),
                Optional.of(MethodHandles.lookup().findStatic(
                        TestFixedWidthHashJoinIntegration.class,
                        "unexpectedPairHash",
                        MethodType.methodType(long.class, Vector.class, int.class))),
                Optional.empty());
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:semantic-hash-join-pair");
            }

            @Override
            public Class<?> carrierType()
            {
                return Object.class;
            }

            @Override
            public TypeOperators operators()
            {
                return operators;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class);
            }
        };
    }

    public static boolean unexpectedPairIdentical(Vector left, int leftPosition, Vector right, int rightPosition)
    {
        throw new AssertionError("row-wise vector identity must not be used by a persistent key table");
    }

    public static long unexpectedPairHash(Vector vector, int position)
    {
        throw new AssertionError("row-wise vector hash must not be used by a persistent key table");
    }
}
