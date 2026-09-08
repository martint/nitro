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
import org.weakref.nitro.core.type.RepeatedKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.ArrayVector;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.MapVector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.StructVector;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMembershipSet
{
    @Test
    void membershipRejectsSemanticKeyWithoutPhysicalLayout()
            throws ReflectiveOperationException
    {
        try (EngineResources engineResources = EngineResources.createDefault();
                Allocator allocator = new Allocator(engineResources)) {
            allocator.beginExecution();
            Allocator.Context allocationContext = new Allocator.Context("semantic-membership");
            TypeBinding semanticType = semanticLongType();
            MembershipSet set = new MembershipSet(
                    allocator,
                    allocationContext,
                    engineResources.operatorResources(),
                    engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                    Optional.of(semanticType));
            try {
                assertThatThrownBy(() -> set.addBatch(new I64Vector(new long[] {1}), null, Mask.all(1)))
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessageContaining("semi-join membership")
                        .hasMessageContaining("provider-described generated persistent key layout")
                        .hasMessageContaining("ADR-0090");
            }
            finally {
                set.releaseBuffers();
                allocator.release(allocationContext);
            }
        }
    }

    @Test
    void fixedWidthProviderLayoutDrivesMembership()
    {
        try (EngineResources engineResources = EngineResources.createDefault();
                Allocator allocator = new Allocator(engineResources)) {
            allocator.beginExecution();
            Allocator.Context allocationContext = new Allocator.Context("fixed-width-membership");
            TypeBinding pairType = fixedWidthPairType();
            MembershipSet set = new MembershipSet(
                    allocator,
                    allocationContext,
                    engineResources.operatorResources(),
                    engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                    Optional.of(pairType));
            try {
                set.addBatch(pairVector(new long[] {1, 2}, new long[] {10, 20}), null, Mask.all(2));
                StructVector probe = pairVector(new long[] {2, 3, 1}, new long[] {20, 30, 11});
                set.beginProbeBatch(probe, null);
                try {
                    assertThat(set.contains(0)).isTrue();
                    assertThat(set.contains(1)).isFalse();
                    assertThat(set.contains(2)).isFalse();
                }
                finally {
                    set.endProbeBatch();
                }
            }
            finally {
                set.releaseBuffers();
                allocator.release(allocationContext);
            }
        }
    }

    @Test
    void orderedRepeatedLayoutDrivesMembership()
    {
        try (EngineResources engineResources = EngineResources.createDefault();
                Allocator allocator = new Allocator(engineResources)) {
            allocator.beginExecution();
            Allocator.Context allocationContext = new Allocator.Context("ordered-repeated-membership");
            MembershipSet set = new MembershipSet(
                    allocator,
                    allocationContext,
                    engineResources.operatorResources(),
                    engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                    Optional.of(orderedRepeatedType()));
            try {
                ArrayVector build = arrays(new long[][] {{1, 2}, {}, {2, 1}});
                set.addBatch(build, null, Mask.all(build.length()));
                ArrayVector probeDomain = arrays(new long[][] {{2, 1}, {1, 3}, {}, {1, 2}});
                Vector probe = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 0}, probeDomain);
                set.beginProbeBatch(probe, null);
                try {
                    assertThat(set.contains(0)).isTrue();
                    assertThat(set.contains(1)).isFalse();
                    assertThat(set.contains(2)).isTrue();
                    assertThat(set.contains(3)).isTrue();
                    assertThat(set.contains(4)).isTrue();
                }
                finally {
                    set.endProbeBatch();
                }
            }
            finally {
                set.releaseBuffers();
                allocator.release(allocationContext);
            }
        }
    }

    @Test
    void orderedRepeatedProductLayoutDrivesMembership()
    {
        TypeBinding rowType = productType(
                rawLongType("testing:ordered-repeated-product-id"),
                rawBinaryType("testing:ordered-repeated-product-label"));
        try (EngineResources engineResources = EngineResources.createDefault();
                Allocator allocator = new Allocator(engineResources)) {
            allocator.beginExecution();
            Allocator.Context allocationContext = new Allocator.Context("ordered-repeated-product-membership");
            MembershipSet set = new MembershipSet(
                    allocator,
                    allocationContext,
                    engineResources.operatorResources(),
                    engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                    Optional.of(orderedRepeatedType(rowType)));
            try {
                ArrayVector build = productArrays(
                        new long[] {1, 2, 0},
                        new String[] {"a", "b", "ignored"},
                        new boolean[] {false, false, true});
                set.addBatch(build, null, Mask.all(build.length()));
                ArrayVector probe = productArrays(
                        new long[] {2, 3, 0, 1, 1},
                        new String[] {"b", "c", "ignored", "x", "a"},
                        new boolean[] {false, false, true, false, false});
                set.beginProbeBatch(probe, null);
                try {
                    assertThat(set.contains(0)).isTrue();
                    assertThat(set.contains(1)).isFalse();
                    assertThat(set.contains(2)).isTrue();
                    assertThat(set.contains(3)).isFalse();
                    assertThat(set.contains(4)).isTrue();
                }
                finally {
                    set.endProbeBatch();
                }
            }
            finally {
                set.releaseBuffers();
                allocator.release(allocationContext);
            }
        }
    }

    @Test
    void unorderedRepeatedLayoutDrivesMembershipIndependentOfEntryOrder()
    {
        try (EngineResources engineResources = EngineResources.createDefault();
                Allocator allocator = new Allocator(engineResources)) {
            allocator.beginExecution();
            Allocator.Context allocationContext = new Allocator.Context("unordered-repeated-membership");
            MembershipSet set = new MembershipSet(
                    allocator,
                    allocationContext,
                    engineResources.operatorResources(),
                    engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                    Optional.of(unorderedRepeatedMapType()));
            try {
                MapVector build = maps(
                        new long[][] {{1, 2}, {}, {3}, {1, 1}},
                        new String[][] {{"a", "b"}, {}, {"ignored"}, {"a", "a"}},
                        new boolean[][] {{false, false}, {}, {true}, {false, false}});
                set.addBatch(build, null, Mask.all(build.length()));
                MapVector probeDomain = maps(
                        new long[][] {{2, 1}, {}, {3}, {1}, {1, 1}, {1, 2}},
                        new String[][] {{"b", "a"}, {}, {"different-ignored"}, {"a"}, {"a", "a"}, {"a", "different"}},
                        new boolean[][] {{false, false}, {}, {true}, {false}, {false, false}, {false, false}});
                Vector probe = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 4, 5, 0}, probeDomain);
                set.beginProbeBatch(probe, null);
                try {
                    assertThat(set.contains(0)).isTrue();
                    assertThat(set.contains(1)).isTrue();
                    assertThat(set.contains(2)).isTrue();
                    assertThat(set.contains(3)).isFalse();
                    assertThat(set.contains(4)).isTrue();
                    assertThat(set.contains(5)).isFalse();
                    assertThat(set.contains(6)).isTrue();
                }
                finally {
                    set.endProbeBatch();
                }
            }
            finally {
                set.releaseBuffers();
                allocator.release(allocationContext);
            }
        }
    }

    @Test
    void canonicalProjectionDrivesEncodedMembership()
    {
        try (EngineResources engineResources = EngineResources.createDefault();
                Allocator allocator = new Allocator(engineResources)) {
            allocator.beginExecution();
            Allocator.Context allocationContext = new Allocator.Context("canonical-projection-membership");
            MembershipSet set = new MembershipSet(
                    allocator,
                    allocationContext,
                    engineResources.operatorResources(),
                    engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                    Optional.of(CanonicalFixedWidthKeyTestType.INSTANCE));
            try {
                I64Vector buildDomain = new I64Vector(new long[] {
                        CanonicalFixedWidthKeyTestType.pack(100, 1),
                        CanonicalFixedWidthKeyTestType.pack(200, 2)});
                Vector build = DictionaryVector.wrap(new int[] {0, 1}, buildDomain);
                set.addBatch(build, null, Mask.all(build.length()));

                Vector probe = new RleVector(
                        new int[] {1, 1, 1},
                        new I64Vector(new long[] {
                                CanonicalFixedWidthKeyTestType.pack(100, 9),
                                CanonicalFixedWidthKeyTestType.pack(300, 7),
                                CanonicalFixedWidthKeyTestType.pack(200, 8)}));
                set.beginProbeBatch(probe, null);
                try {
                    assertThat(set.contains(0)).isTrue();
                    assertThat(set.contains(1)).isFalse();
                    assertThat(set.contains(2)).isTrue();
                }
                finally {
                    set.endProbeBatch();
                }
            }
            finally {
                set.releaseBuffers();
                allocator.release(allocationContext);
            }
        }
    }

    @Test
    void membershipRejectsVectorOutsidePlanTimeTypeBinding()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        TypeBinding i32Only = new TestingTypeBinding(new TypeIdentity("testing:i32-only"), Set.of(I32Vector.class));
        MembershipSet set = new MembershipSet(
                allocator,
                allocationContext,
                engineResources.operatorResources(),
                engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                Optional.of(i32Only));
        try {
            assertThatThrownBy(() -> set.addBatch(
                    new I64Vector(new long[] {1}),
                    null,
                    Mask.all(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("testing:i32-only");
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void exactLongMembershipSurvivesRangeExpansionAndEncodedProbe()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(
                allocator,
                allocationContext,
                engineResources.operatorResources(),
                engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                Optional.empty());
        try {
            long[] initial = new long[1_024];
            for (int index = 0; index < initial.length; index++) {
                initial[index] = 100_000L + index * 100L;
            }
            set.addBatch(new I64Vector(initial), null, Mask.all(initial.length));
            set.addBatch(new I64Vector(new long[] {-10_000, 240_000}), null, Mask.all(2));

            I32Vector dictionary = new I32Vector(new int[] {100_000, -10_000, 240_000, 7});
            DictionaryVector probe = DictionaryVector.wrap(new int[] {0, 1, 2, 3, 0}, dictionary);
            BooleanVector nulls = new BooleanVector(new boolean[] {false, false, false, false, true});
            set.beginProbeBatch(probe, nulls);
            try {
                assertThat(set.contains(0)).isTrue();
                assertThat(set.contains(1)).isTrue();
                assertThat(set.contains(2)).isTrue();
                assertThat(set.contains(3)).isFalse();
                assertThat(set.contains(4)).isFalse();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void sparseExtremeLongDomainFallsBackWithoutLosingExactness()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(
                allocator,
                allocationContext,
                engineResources.operatorResources(),
                new MembershipSetPolicy(64, 64, 1),
                Optional.empty());
        try {
            set.addBatch(
                    new I64Vector(new long[] {Long.MIN_VALUE, 0, Long.MAX_VALUE}),
                    null,
                    Mask.all(3));
            I64Vector probe = new I64Vector(new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE});
            set.beginProbeBatch(probe, null);
            try {
                assertThat(set.contains(0)).isTrue();
                assertThat(set.contains(1)).isFalse();
                assertThat(set.contains(2)).isTrue();
                assertThat(set.contains(3)).isFalse();
                assertThat(set.contains(4)).isTrue();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void denseLongMembershipConvertsToHashWhenLaterKeysEscapeTheBoundedDomain()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(
                allocator,
                allocationContext,
                engineResources.operatorResources(),
                engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                Optional.empty());
        try {
            set.addBatch(new I64Vector(new long[] {1, 2, 2, 3}), null, Mask.all(4));
            set.addBatch(new I64Vector(new long[] {100_000_000}), null, Mask.all(1));

            I64Vector probe = new I64Vector(new long[] {0, 1, 2, 3, 4, 100_000_000});
            set.beginProbeBatch(probe, null);
            try {
                assertThat(set.contains(0)).isFalse();
                assertThat(set.contains(1)).isTrue();
                assertThat(set.contains(2)).isTrue();
                assertThat(set.contains(3)).isTrue();
                assertThat(set.contains(4)).isFalse();
                assertThat(set.contains(5)).isTrue();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void preparedLongMembershipSupportsIndependentProbeViews()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet owner = new MembershipSet(
                allocator,
                allocationContext,
                engineResources.operatorResources(),
                engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                Optional.empty());
        try {
            owner.addBatch(new I64Vector(new long[] {10, 1_000_000, 20}), null, Mask.all(3));
            MembershipSet first = owner.newProbeView().orElseThrow();
            MembershipSet second = owner.newProbeView().orElseThrow();
            try {
                I64Vector firstProbe = new I64Vector(new long[] {10, 11});
                I64Vector secondProbe = new I64Vector(new long[] {1_000_000, 2_000_000});
                first.beginProbeBatch(firstProbe, null);
                second.beginProbeBatch(secondProbe, null);
                assertThat(first.contains(0)).isTrue();
                assertThat(second.contains(0)).isTrue();
                assertThat(first.contains(1)).isFalse();
                assertThat(second.contains(1)).isFalse();
                first.endProbeBatch();
                second.endProbeBatch();
            }
            finally {
                first.releaseBuffers();
                second.releaseBuffers();
            }

            owner.beginProbeBatch(new I64Vector(new long[] {20}), null);
            try {
                assertThat(owner.contains(0)).isTrue();
            }
            finally {
                owner.endProbeBatch();
            }
        }
        finally {
            owner.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    @Test
    void emptyBuildRejectsEveryProbe()
    {
        EngineResources engineResources = EngineResources.createDefault();
        Allocator allocator = new Allocator(engineResources);
        Allocator.Context allocationContext = new Allocator.Context("TestMembershipSet");
        MembershipSet set = new MembershipSet(
                allocator,
                allocationContext,
                engineResources.operatorResources(),
                engineResources.operatorResources().semiJoinPolicy().membershipSet(),
                Optional.empty());
        try {
            set.beginProbeBatch(new I64Vector(new long[] {1, 2}), null);
            try {
                assertThat(set.contains(0)).isFalse();
                assertThat(set.contains(1)).isFalse();
            }
            finally {
                set.endProbeBatch();
            }
        }
        finally {
            set.releaseBuffers();
            allocator.release(allocationContext);
        }
    }

    private record TestingTypeBinding(TypeIdentity identity, Set<Class<? extends Vector>> supportedVectorTypes)
            implements TypeBinding
    {
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
    }

    private static StructVector pairVector(long[] high, long[] low)
    {
        StructVector vector = new StructVector(high.length);
        vector.setField("high", Streams.ofValues(new I64Vector(high)));
        vector.setField("low", Streams.ofValues(new I64Vector(low)));
        return vector;
    }

    private static ArrayVector arrays(long[][] rows)
    {
        int elementCount = java.util.Arrays.stream(rows).mapToInt(row -> row.length).sum();
        long[] elements = new long[elementCount];
        ArrayVector result = new ArrayVector(rows.length);
        int offset = 0;
        for (int row = 0; row < rows.length; row++) {
            result.offsets()[row] = offset;
            System.arraycopy(rows[row], 0, elements, offset, rows[row].length);
            offset += rows[row].length;
        }
        result.offsets()[rows.length] = offset;
        result.setElements(Streams.ofValues(new I64Vector(elements)));
        return result;
    }

    private static ArrayVector productArrays(long[] ids, String[] labels, boolean[] nullRows)
    {
        assertThat(labels.length).isEqualTo(ids.length);
        assertThat(nullRows.length).isEqualTo(ids.length);
        StructVector rows = new StructVector(ids.length);
        rows.setField("id", Streams.ofValues(new I64Vector(ids)));
        rows.setField("label", Streams.ofValues(utf8(labels)));
        ArrayVector result = new ArrayVector(ids.length);
        for (int position = 0; position <= ids.length; position++) {
            result.offsets()[position] = position;
        }
        result.setElements(Streams.builder()
                .put(org.weakref.nitro.data.Stream.VALUES, rows)
                .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(nullRows))
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
                        .put(org.weakref.nitro.data.Stream.VALUES, utf8(flatValues))
                        .put(org.weakref.nitro.data.Stream.NULLS, new BooleanVector(flatNulls))
                        .build());
        return result;
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

    private static TypeBinding orderedRepeatedType()
    {
        TypeBinding elements = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:ordered-repeated-membership-element");
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
                return Set.of(I64Vector.class, DictionaryVector.class, RleVector.class);
            }
        };
        return orderedRepeatedType(elements);
    }

    private static TypeBinding orderedRepeatedType(TypeBinding elements)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:ordered-repeated-membership");
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
                return Set.of(ArrayVector.class, DictionaryVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding productType(TypeBinding id, TypeBinding label)
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:ordered-repeated-product");
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
                return List.of(id, label);
            }

            @Override
            public Optional<PersistentKeyLayout> persistentKeyLayout()
            {
                return Optional.of(new PersistentKeyLayout(List.of(
                        new PersistentKeyLayout.Field(List.of("id"), id),
                        new PersistentKeyLayout.Field(List.of("label"), label))));
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(StructVector.class, DictionaryVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding rawBinaryType(String identity)
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

    private static TypeBinding unorderedRepeatedMapType()
    {
        TypeBinding keys = rawLongType("testing:unordered-repeated-membership-key");
        TypeBinding values = new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:unordered-repeated-membership-value");
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
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:unordered-repeated-membership");
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
                return Set.of(MapVector.class, DictionaryVector.class, RleVector.class);
            }
        };
    }

    private static TypeBinding rawLongType(String identity)
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
            public boolean supportsRawKeyIdentity()
            {
                return true;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class, DictionaryVector.class, RleVector.class);
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
                return new TypeIdentity("testing:fixed-width-membership-pair");
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
                return Set.of(StructVector.class);
            }
        };
    }

    private static TypeBinding semanticLongType()
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
                        TestMembershipSet.class,
                        "longIdentical",
                        MethodType.methodType(boolean.class, Vector.class, int.class, Vector.class, int.class))),
                Optional.of(MethodHandles.lookup().findStatic(
                        TestMembershipSet.class,
                        "longHash",
                        MethodType.methodType(long.class, Vector.class, int.class))),
                Optional.empty());
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("testing:semantic-membership");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return operators;
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I64Vector.class);
            }
        };
    }

    public static boolean longIdentical(Vector left, int leftPosition, Vector right, int rightPosition)
    {
        return ((I64Vector) left).values()[leftPosition] == ((I64Vector) right).values()[rightPosition];
    }

    public static long longHash(Vector vector, int position)
    {
        return Long.hashCode(((I64Vector) vector).values()[position]);
    }
}
