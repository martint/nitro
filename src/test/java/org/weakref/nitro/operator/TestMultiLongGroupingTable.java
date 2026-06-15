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
