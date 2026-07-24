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
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.function.scalar.builtin.VectorAccess;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the {@link MultiLongGroupingTableGenerator}-generated tables directly (independent of the
 * grouping operator): group ids are first-seen order, equal tuples collide, the reverse map matches, and
 * nulls + rehash work — for several arities.
 */
class TestMultiLongGroupingTable
{
    private final EngineResources engineResources = EngineResources.createDefault();
    private final PrimitiveArrayPool arrayPool = engineResources.primitiveArrays();
    private final OperatorCodeGenerationResources codeGeneration = engineResources.operatorCodeGeneration();

    @Test
    void testAllAritiesAgainstReferenceModel()
    {
        for (int arity = 2; arity <= AbstractMultiLongGroupingTable.MAX_ARITY; arity++) {
            assertMatchesReference(arity, 5_000, 37, 7L, 0.0);
            assertMatchesReference(arity, 5_000, 37, 11L, 0.2); // with nulls
        }
    }

    @Test
    void testDistinctOnlyTableRehashesWithoutGroupPayload()
    {
        int arity = 7;
        int rows = 5_000;
        VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
        VectorAccess.BooleanValues[] nullAccessors = new VectorAccess.BooleanValues[arity];
        for (int column = 0; column < arity; column++) {
            int multiplier = column + 1;
            keyAccessors[column] = position -> (long) position * multiplier;
            nullAccessors[column] = _ -> false;
        }
        int[] positions = new int[rows];
        for (int position = 0; position < rows; position++) {
            positions[position] = position;
        }

        AbstractMultiLongGroupingTable table = codeGeneration.multiLongGrouping().createDistinct(arity, 16, arrayPool);
        assertThat(table.stride).isEqualTo(arity);
        assertThat(table.keysByGroup).isNull();
        assertThat(table.assignDistinctBatchNullFree(keyAccessors, nullAccessors, positions, rows, positions, 0))
                .isEqualTo(rows);
        assertThat(table.assignDistinctBatchNullFree(keyAccessors, nullAccessors, positions, rows, positions, table.size))
                .isZero();
        table.releaseBuffers();
    }

    @Test
    void testAdaptiveCompactTableAllAritiesAndExactWidePromotion()
    {
        for (int arity = 2; arity <= AbstractMultiLongGroupingTable.MAX_ARITY; arity++) {
            int rows = 2_000;
            long[][] keys = new long[arity][rows];
            boolean[][] nulls = new boolean[arity][rows];
            VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
            VectorAccess.BooleanValues[] nullAccessors = new VectorAccess.BooleanValues[arity];
            for (int column = 0; column < arity; column++) {
                for (int row = 0; row < rows; row++) {
                    keys[column][row] = (long) (row % 97) * (column + 1);
                    nulls[column][row] = row % (31 + column) == 0;
                }
                long[] columnKeys = keys[column];
                boolean[] columnNulls = nulls[column];
                keyAccessors[column] = position -> columnKeys[position];
                nullAccessors[column] = position -> columnNulls[position];
            }
            int[] positions = new int[rows];
            for (int row = 0; row < rows; row++) {
                positions[row] = row;
            }
            long[] compactResult = new long[rows];
            long[] referenceResult = new long[rows];
            LongGroupingTable compact = AdaptiveLongGroupingTable.create(arity, 16, arrayPool, codeGeneration);
            LongGroupingTable reference = codeGeneration.multiLongGrouping().create(arity, 16, arrayPool);
            long compactCount = compact.assignBatch(keyAccessors, nullAccessors, positions, rows, compactResult, 0);
            long referenceCount = reference.assignBatch(keyAccessors, nullAccessors, positions, rows, referenceResult, 0);
            assertThat(compactResult).isEqualTo(referenceResult);
            assertThat(compactCount).isEqualTo(referenceCount);

            // A later wide value must promote all prior groups without changing their ids or null semantics.
            keys[arity - 1][rows - 1] = 1L << 40;
            compactCount = compact.assignBatch(keyAccessors, nullAccessors, positions, rows, compactResult, compactCount);
            referenceCount = reference.assignBatch(keyAccessors, nullAccessors, positions, rows, referenceResult, referenceCount);
            assertThat(compactResult).isEqualTo(referenceResult);
            assertThat(compactCount).isEqualTo(referenceCount);
            for (int groupId = 0; groupId < compactCount; groupId++) {
                for (int column = 0; column < arity; column++) {
                    assertThat(compact.groupedValueIsNull(column, groupId))
                            .isEqualTo(reference.groupedValueIsNull(column, groupId));
                    if (!compact.groupedValueIsNull(column, groupId)) {
                        assertThat(compact.groupedValue(column, groupId)).isEqualTo(reference.groupedValue(column, groupId));
                    }
                }
            }
            compact.releaseBuffers();
            reference.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveGroupingPairSkipsShortLivedPreTerminalGeneration()
    {
        AdaptiveLongGroupingTable groupingPair = AdaptiveLongGroupingTable.create(2, 16, arrayPool, codeGeneration);
        AdaptiveLongGroupingTable distinctPair = AdaptiveLongGroupingTable.createDistinct(2, 16, arrayPool, codeGeneration);
        AdaptiveLongGroupingTable groupingTriple = AdaptiveLongGroupingTable.create(3, 16, arrayPool, codeGeneration);
        int quarterTerminalCapacity = 1 << 22;

        assertThat(groupingPair.terminalReuseEligible(quarterTerminalCapacity)).isTrue();
        assertThat(distinctPair.terminalReuseEligible(quarterTerminalCapacity)).isFalse();
        assertThat(groupingTriple.terminalReuseEligible(quarterTerminalCapacity)).isFalse();

        groupingPair.releaseBuffers();
        distinctPair.releaseBuffers();
        groupingTriple.releaseBuffers();
    }

    @Test
    void testAdaptiveNullFreeGeneratedKernelAndWidePromotion()
    {
        for (int arity = 2; arity <= AbstractMultiLongGroupingTable.MAX_ARITY; arity++) {
            int rows = 2_000;
            long[][] keys = new long[arity][rows];
            VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
            for (int column = 0; column < arity; column++) {
                for (int row = 0; row < rows; row++) {
                    keys[column][row] = (long) (row % 193) * (column + 1);
                }
                long[] columnKeys = keys[column];
                keyAccessors[column] = position -> columnKeys[position];
            }
            int[] positions = new int[rows];
            for (int row = 0; row < rows; row++) {
                positions[row] = row;
            }
            long[] compactResult = new long[rows];
            long[] referenceResult = new long[rows];
            VectorAccess.BooleanValues[] nonNullAccessors = new VectorAccess.BooleanValues[arity];
            for (int column = 0; column < arity; column++) {
                nonNullAccessors[column] = ignored -> false;
            }
            LongGroupingTable compact = AdaptiveLongGroupingTable.create(arity, 16, arrayPool, codeGeneration);
            LongGroupingTable reference = codeGeneration.multiLongGrouping().create(arity, 16, arrayPool);
            compact.ensureCapacity(rows);
            long compactCount = compact.assignBatch(keyAccessors, null, null, rows, compactResult, 0);
            long referenceCount = reference.assignBatch(keyAccessors, nonNullAccessors, positions, rows, referenceResult, 0);
            assertThat(compactResult).isEqualTo(referenceResult);
            assertThat(compactCount).isEqualTo(referenceCount);

            keys[arity - 1][rows - 1] = 1L << 40;
            compactCount = compact.assignBatch(keyAccessors, null, null, rows, compactResult, compactCount);
            referenceCount = reference.assignBatch(keyAccessors, nonNullAccessors, positions, rows, referenceResult, referenceCount);
            assertThat(compactResult).isEqualTo(referenceResult);
            assertThat(compactCount).isEqualTo(referenceCount);
            compact.releaseBuffers();
            reference.releaseBuffers();
        }
    }

    @Test
    void testAdaptiveDirectDistinctKernelDenseSparseAndPromotion()
    {
        int arity = 3;
        long[][] keys = {
                {10, 10, 20, 10, 30, 20, 40, 50},
                {1, 1, 2, 1, 3, 2, 4, 5},
                {7, 7, 8, 7, 9, 8, 10, 11},
        };
        VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
        for (int column = 0; column < arity; column++) {
            long[] columnKeys = keys[column];
            keyAccessors[column] = position -> columnKeys[position];
        }

        AdaptiveLongGroupingTable table = AdaptiveLongGroupingTable.createDistinct(arity, 16, arrayPool, codeGeneration);
        int[] distinctPositions = new int[keys[0].length];

        long nextGroupId = table.assignDistinctBatch(
                keyAccessors, null, 6, keys[0].length, distinctPositions, 0);
        assertThat(nextGroupId).isEqualTo(3);
        assertThat(distinctPositions).startsWith(0, 2, 4);

        int[] sparsePositions = {1, 5, 6, 7};
        nextGroupId = table.assignDistinctBatch(
                keyAccessors, sparsePositions, sparsePositions.length, keys[0].length, distinctPositions, nextGroupId);
        assertThat(nextGroupId).isEqualTo(5);
        assertThat(distinctPositions).startsWith(6, 7);

        // A wide value after an already-inserted compact prefix forces promotion. The replay must neither
        // duplicate that prefix nor lose its first-seen position, and subsequent promoted batches must retain
        // the same direct-distinct contract.
        keys[0][1] = 60;
        keys[1][1] = 6;
        keys[2][1] = 12;
        keys[0][3] = 1L << 40;
        keys[1][3] = 13;
        keys[2][3] = 14;
        int[] promotionPositions = {1, 3, 3};
        nextGroupId = table.assignDistinctBatch(
                keyAccessors, promotionPositions, promotionPositions.length, keys[0].length, distinctPositions, nextGroupId);
        assertThat(nextGroupId).isEqualTo(7);
        assertThat(distinctPositions).startsWith(1, 3);

        int[] promotedPositions = {0, 3, 1, 4};
        nextGroupId = table.assignDistinctBatch(
                keyAccessors, promotedPositions, promotedPositions.length, keys[0].length, distinctPositions, nextGroupId);
        assertThat(nextGroupId).isEqualTo(7);
        table.releaseBuffers();
    }

    @Test
    void testAdaptiveDistinctGroupedProbePreservesLinearOrderAndWrap()
    {
        AdaptiveLongGroupingTable table = AdaptiveLongGroupingTable.createDistinct(2, 800_000, arrayPool, codeGeneration);
        int fragment = 0xA5;
        int mismatch = 0x81 << 24 | 1;
        int last = table.slots.length - 1;

        for (int slot = 64; slot < 71; slot++) {
            table.slots[slot] = mismatch;
        }
        table.slots[71] = fragment << 24 | 2;
        assertThat(table.nextProbeCandidate(table.slots, 64, table.slotMask, fragment)).isEqualTo(71);

        for (int slot = last - 3; slot <= last; slot++) {
            table.slots[slot] = mismatch;
        }
        table.slots[0] = mismatch;
        table.slots[1] = fragment << 24 | 3;
        assertThat(table.nextProbeCandidate(table.slots, last - 3, table.slotMask, fragment)).isEqualTo(1);

        table.slots[last] = 0;
        assertThat(table.nextProbeCandidate(table.slots, last - 1, table.slotMask, fragment)).isEqualTo(last);
        table.releaseBuffers();
    }

    @Test
    void testAdaptivePerColumnNullFreeAccessors()
    {
        int arity = 5;
        int rows = 2_000;
        long[][] keys = new long[arity][rows];
        boolean[][] nulls = new boolean[arity][rows];
        VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
        VectorAccess.BooleanValues[] partialNullAccessors = new VectorAccess.BooleanValues[arity];
        VectorAccess.BooleanValues[] referenceNullAccessors = new VectorAccess.BooleanValues[arity];
        for (int column = 0; column < arity; column++) {
            for (int row = 0; row < rows; row++) {
                keys[column][row] = (long) (row % 127) * (column + 1);
                nulls[column][row] = (column & 1) != 0 && row % (29 + column) == 0;
            }
            long[] columnKeys = keys[column];
            boolean[] columnNulls = nulls[column];
            keyAccessors[column] = position -> columnKeys[position];
            referenceNullAccessors[column] = position -> columnNulls[position];
            if ((column & 1) != 0) {
                partialNullAccessors[column] = referenceNullAccessors[column];
            }
        }
        int[] positions = new int[rows];
        for (int row = 0; row < rows; row++) {
            positions[row] = row;
        }
        long[] compactResult = new long[rows];
        long[] referenceResult = new long[rows];
        LongGroupingTable compact = AdaptiveLongGroupingTable.create(arity, 16, arrayPool, codeGeneration);
        LongGroupingTable reference = codeGeneration.multiLongGrouping().create(arity, 16, arrayPool);
        compact.ensureCapacity(rows);
        long compactCount = compact.assignBatch(keyAccessors, partialNullAccessors, null, rows, compactResult, 0);
        long referenceCount = reference.assignBatch(keyAccessors, referenceNullAccessors, positions, rows, referenceResult, 0);
        assertThat(compactResult).isEqualTo(referenceResult);
        assertThat(compactCount).isEqualTo(referenceCount);

        keys[arity - 1][rows - 1] = 1L << 40;
        compactCount = compact.assignBatch(keyAccessors, partialNullAccessors, null, rows, compactResult, compactCount);
        referenceCount = reference.assignBatch(keyAccessors, referenceNullAccessors, positions, rows, referenceResult, referenceCount);
        assertThat(compactResult).isEqualTo(referenceResult);
        assertThat(compactCount).isEqualTo(referenceCount);
        compact.releaseBuffers();
        reference.releaseBuffers();
    }

    private void assertMatchesReference(int arity, int rows, int distinctPerColumn, long seed, double nullFraction)
    {
        Random random = new Random(seed);
        // Build the key columns and null flags.
        long[][] keys = new long[arity][rows];
        boolean[][] nulls = new boolean[arity][rows];
        for (int column = 0; column < arity; column++) {
            for (int row = 0; row < rows; row++) {
                nulls[column][row] = random.nextDouble() < nullFraction;
                keys[column][row] = random.nextInt(distinctPerColumn);
            }
        }

        VectorAccess.LongValues[] keyAccessors = new VectorAccess.LongValues[arity];
        VectorAccess.BooleanValues[] nullAccessors = new VectorAccess.BooleanValues[arity];
        for (int column = 0; column < arity; column++) {
            long[] columnKeys = keys[column];
            boolean[] columnNulls = nulls[column];
            keyAccessors[column] = position -> columnKeys[position];
            nullAccessors[column] = position -> columnNulls[position];
        }

        int[] positions = new int[rows];
        for (int row = 0; row < rows; row++) {
            positions[row] = row;
        }
        long[] result = new long[rows];

        AbstractMultiLongGroupingTable table = codeGeneration.multiLongGrouping().create(arity, 16, arrayPool);
        long nextGroupId = table.assignBatch(keyAccessors, nullAccessors, positions, rows, result, 0L);

        // Reference: first-seen ids over the canonical (nullMask, key-or-0) tuple.
        Map<String, Long> referenceIds = new HashMap<>();
        long expectedNext = 0;
        for (int row = 0; row < rows; row++) {
            StringBuilder canonical = new StringBuilder();
            for (int column = 0; column < arity; column++) {
                boolean isNull = nulls[column][row];
                canonical.append(isNull ? "N" : Long.toString(keys[column][row])).append('|');
            }
            String tuple = canonical.toString();
            Long expectedId = referenceIds.get(tuple);
            if (expectedId == null) {
                expectedId = expectedNext++;
                referenceIds.put(tuple, expectedId);
            }
            assertThat(result[row]).as("arity=%d row=%d", arity, row).isEqualTo(expectedId);
        }
        assertThat(nextGroupId).as("group count, arity=%d", arity).isEqualTo(expectedNext);

        // Reverse map: each group's stored key/null must reconstruct the tuple that created it.
        for (int row = 0; row < rows; row++) {
            int group = (int) result[row];
            byte nullMask = table.nullMasksByGroup[group];
            for (int column = 0; column < arity; column++) {
                boolean storedNull = (nullMask & (1 << column)) != 0;
                assertThat(storedNull).as("arity=%d group=%d col=%d nullness", arity, group, column)
                        .isEqualTo(nulls[column][row]);
                if (!storedNull) {
                    assertThat(table.keysByGroup[column][group]).as("arity=%d group=%d col=%d value", arity, group, column)
                            .isEqualTo(keys[column][row]);
                }
            }
        }
    }
}
