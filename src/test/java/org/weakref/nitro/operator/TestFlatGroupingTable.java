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
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.execution.EngineResources;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFlatGroupingTable
{
    private final EngineResources engineResources = EngineResources.createDefault();
    private final PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
    private final OperatorCodeGenerationResources codeGeneration = engineResources.operatorCodeGeneration();
    private final GroupingStateResources groupingResources = engineResources.groupingState();
    private final AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy = engineResources.operatorResources().adaptiveLongGroupingPolicy();
    private final FlatKeyTablePolicy flatKeyTablePolicy = engineResources.operatorResources().flatKeyTablePolicy();

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
                // fields through resolved long accessors rather than eagerly pre-hashing every dictionary entry.
                DictionaryVector.wrap(ids, size, new I64Vector(largeLongDictionary)),
                DictionaryVector.wrap(ids, size, utf8("a", "b", "c")),
                DictionaryVector.wrap(ids, size, new I64Vector(largeLongDictionary.clone())),
                DictionaryVector.wrap(ids, size, utf8("d", "e", "f")),
                DictionaryVector.wrap(ids, size, utf8("g", "h", "i"))};
        Vector[] nulls = {
                null,
                new BooleanVector(allNullValues),
                new BooleanVector(mixedNullValues),
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
                            position)).isEqualTo(DictionaryRecordEqualityKernel.IDENTICAL);
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
            assertThat(fallbackLayout.usesGeneratedDictionaryRecordEquality()).isFalse();
            long nextGroup = 0;
            for (int position = 0; position < distinct; position++) {
                long group = fallbackRecords.assignGroup(flatValues, nulls, position, nextGroup);
                assertThat(group).isEqualTo(position);
                nextGroup++;
            }
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
        assertThat(FlatKeyLayout.tryCreate(flat, arrayPool, codeGeneration, flatKeyTablePolicy).usesCompactEmbeddedBinaryRecords()).isFalse();
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
            assertThat(table.assignGroup(first, null, 0, 0)).isEqualTo(0);
            assertThat(table.assignGroup(first, null, 1, 1)).isEqualTo(1);
            assertThat(table.assignGroup(first, null, 2, 2)).isEqualTo(0);
            table.endBatch();

            // Flat input has no query-stable dictionary id. Existing id-backed records must still compare by
            // exact bytes, while a new record uses the compact layout's exact fallback metadata.
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
            }
        }
        finally {
            table.releaseBuffers();
            allocator.release(context);
        }
    }

    @Test
    void testSingleDictionaryGroupCacheObservesPooledContentGeneration()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context context = new Allocator.Context("dictionary-group-cache-generation");
        BinaryVector first = binary(allocator, context, "alpha", "beta");
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
            BinaryVector reused = binary(allocator, context, "gamma", "zeta");
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
    void testNormalizedIntKeyPreservesCompleteEqualityAcrossBinaryEncodings()
    {
        Vector[] firstValues = {
                DictionaryVector.wrapNested(new int[] {0, 0, 1, 0}, 4, utf8("alpha", "beta")),
                new I64Vector(new long[] {1, 1, 2, 1}),
                new I64Vector(new long[] {10, 10, 20, 10}),
                new I64Vector(new long[] {1, 1, 1, 2})};
        Vector[] nulls = {null, null, null, null};
        FlatKeyLayout layout = FlatKeyLayout.tryCreate(firstValues, true, arrayPool, codeGeneration, flatKeyTablePolicy);
        FlatGroupingTable table = new FlatGroupingTable(layout, 2, true);
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
                    new I64Vector(new long[] {20, -10}),
                    new I64Vector(new long[] {1, 1})};
            table.beginBatch(laterValues, nulls);
            table.prepareBatchHashes(laterValues, nulls, Mask.all(2));
            assertThat(table.assignGroup(laterValues, nulls, 0, 3)).isEqualTo(1);
            assertThat(table.assignGroup(laterValues, nulls, 1, 3)).isEqualTo(3);
            table.endBatch();
        }
        finally {
            table.releaseBuffers();
        }
    }

    @Test
    void testNormalizedIntKeyEligibilityTransitionPreservesExistingGroups()
    {
        Vector[] firstValues = {
                DictionaryVector.wrapNested(new int[] {0, 1}, 2, utf8("alpha", "outside-domain")),
                new I64Vector(new long[] {1, -1}),
                new I64Vector(new long[] {10, 20}),
                new I64Vector(new long[] {100, 200})};
        Vector[] nulls = {null, null, null, null};
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
                    new I64Vector(new long[] {10}),
                    new I64Vector(new long[] {100})};
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
}
