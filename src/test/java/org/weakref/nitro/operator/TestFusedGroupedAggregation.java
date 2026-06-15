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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.Sum;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the generated fused single-long-key path in {@link GroupedAggregationOperator} (flat I64,
 * null-free) against a reference map. Covers the multi-accumulator set ({@code SUM, COUNT(*)}), the
 * high-cardinality crossover where the operator transitions from the fused pass to the staged pass, and
 * per-batch fallback when a value column carries nulls. The broader suites don't hit these shapes.
 */
class TestFusedGroupedAggregation
{
    @Test
    void fusedSingleLongSumMatchesReference()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = buildNullFreePages(200_000, 3_000, 4_096, reference);
        assertGroupedSumAndCount(pages, List.of(new Sum(1)), reference, false);
    }

    @Test
    void fusedSumAndCountMatchesReference()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = buildNullFreePages(200_000, 3_000, 4_096, reference);
        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountAll()), reference, true);
    }

    @Test
    void fusedHighCardinalityCrossesToStaged()
    {
        // More distinct groups than GroupedAggregationOperator.FUSE_GROUP_LIMIT (1<<15), so the operator
        // starts on the fused pass and transitions to the staged pass mid-query — the transition must keep
        // the already-fused groups intact and initialize the newly revealed ones correctly.
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = buildNullFreePages(400_000, 50_000, 8_192, reference);
        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountAll()), reference, true);
    }

    @Test
    void fusedFallsBackOnNullValueBatches()
    {
        // Alternate null-free batches (fused) with batches whose value column has nulls (staged fallback),
        // so the operator interleaves the two paths. Sum skips nulls; the reference does too.
        int rows = 120_000;
        int groups = 2_000;
        int batch = 4_096;

        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        int batchIndex = 0;
        for (int start = 0; start < rows; start += batch, batchIndex++) {
            int size = Math.min(batch, rows - start);
            boolean nullableBatch = (batchIndex % 2) == 1;
            long[] keys = new long[size];
            long[] values = new long[size];
            boolean[] valueNulls = new boolean[size];
            for (int index = 0; index < size; index++) {
                long row = start + index;
                long key = row % groups;
                long value = row * 7 - 3;
                keys[index] = key;
                values[index] = value;
                boolean isNull = nullableBatch && (index % 3 == 0);
                valueNulls[index] = isNull;
                long[] state = reference.computeIfAbsent(key, k -> new long[2]);
                if (!isNull) {
                    state[0] += value;
                    state[1] += 1;
                }
                else {
                    // COUNT(*) counts the row regardless of the value being null.
                    state[1] += 1;
                }
            }
            Streams keyStream = Streams.ofValues(new I64Vector(keys));
            Streams valueStream = nullableBatch
                    ? Streams.ofValuesAndNulls(new I64Vector(values), new BooleanVector(valueNulls))
                    : Streams.ofValues(new I64Vector(values));
            pages.add(new TableOperator.Page(size, new Streams[] {keyStream, valueStream}, Mask.all(size)));
        }

        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountAll()), reference, true);
    }

    private static List<TableOperator.Page> buildNullFreePages(int rows, int groups, int batch, Map<Long, long[]> reference)
    {
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < rows; start += batch) {
            int size = Math.min(batch, rows - start);
            long[] keys = new long[size];
            long[] values = new long[size];
            for (int index = 0; index < size; index++) {
                long row = start + index;
                long key = row % groups;
                long value = row * 7 - 3;
                keys[index] = key;
                values[index] = value;
                long[] state = reference.computeIfAbsent(key, k -> new long[2]);
                state[0] += value;
                state[1] += 1;
            }
            pages.add(TableOperator.Page.values(size, new Vector[] {new I64Vector(keys), new I64Vector(values)}, Mask.all(size)));
        }
        return pages;
    }

    private static void assertGroupedSumAndCount(
            List<TableOperator.Page> pages,
            List<Accumulator> aggregations,
            Map<Long, long[]> reference,
            boolean checkCount)
    {
        Allocator allocator = new Allocator();
        Operator operator = new GroupedAggregationOperator(allocator, List.of(0), aggregations, new TableOperator(2, pages));

        Map<Long, Long> actualSum = new HashMap<>();
        Map<Long, Long> actualCount = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (var result = operator.next()) {
                    Mask mask = result.borrowMask();
                    I64Vector keyColumn = (I64Vector) result.output(0).borrow(Stream.VALUES);
                    I64Vector sumColumn = (I64Vector) result.output(1).borrow(Stream.VALUES);
                    I64Vector countColumn = checkCount ? (I64Vector) result.output(2).borrow(Stream.VALUES) : null;
                    for (int position : mask) {
                        long key = keyColumn.values()[position];
                        actualSum.put(key, sumColumn.values()[position]);
                        if (checkCount) {
                            actualCount.put(key, countColumn.values()[position]);
                        }
                    }
                }
            }
        }

        Map<Long, Long> expectedSum = new HashMap<>();
        Map<Long, Long> expectedCount = new HashMap<>();
        reference.forEach((key, state) -> {
            expectedSum.put(key, state[0]);
            expectedCount.put(key, state[1]);
        });

        assertThat(actualSum).isEqualTo(expectedSum);
        if (checkCount) {
            assertThat(actualCount).isEqualTo(expectedCount);
        }
    }
}
