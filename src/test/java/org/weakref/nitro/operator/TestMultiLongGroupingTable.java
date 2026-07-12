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

        AbstractMultiLongGroupingTable table = MultiLongGroupingTableGenerator.createDistinct(arity, 16);
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
            LongGroupingTable compact = AdaptiveLongGroupingTable.create(arity, 16);
            LongGroupingTable reference = MultiLongGroupingTableGenerator.create(arity, 16);
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
            LongGroupingTable compact = AdaptiveLongGroupingTable.create(arity, 16);
            LongGroupingTable reference = MultiLongGroupingTableGenerator.create(arity, 16);
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
        LongGroupingTable compact = AdaptiveLongGroupingTable.create(arity, 16);
        LongGroupingTable reference = MultiLongGroupingTableGenerator.create(arity, 16);
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

    private static void assertMatchesReference(int arity, int rows, int distinctPerColumn, long seed, double nullFraction)
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

        AbstractMultiLongGroupingTable table = MultiLongGroupingTableGenerator.create(arity, 16);
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
