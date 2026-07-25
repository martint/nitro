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
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.FilteredAccumulator;
import org.weakref.nitro.operator.aggregation.Sum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the generated fused single-long-key path in {@link GroupedAggregationOperator} (flat I32/I64 keys and
 * values) against a reference map. Covers the multi-accumulator set ({@code SUM, COUNT(*)}), the
 * high-cardinality crossover where the operator transitions from the fused pass to the staged pass, and
 * nullable-input fused path. The broader suites don't hit these shapes.
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
    void fusedIntKeyAndValueShapeMatchesReference()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < 100_000; start += 4_096) {
            int size = Math.min(4_096, 100_000 - start);
            int[] keys = new int[size];
            int[] values = new int[size];
            for (int index = 0; index < size; index++) {
                int row = start + index;
                int key = row % 2_000;
                int value = row * 7 - 3;
                keys[index] = key;
                values[index] = value;
                long[] state = reference.computeIfAbsent((long) key, ignored -> new long[2]);
                state[0] += value;
                state[1]++;
            }
            pages.add(TableOperator.Page.values(size, new Vector[] {new I32Vector(keys), new I32Vector(values)}, Mask.all(size)));
        }
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
    void fusedMappedOnlyKeysCrossToStagedBeyondLocalBoundary()
    {
        // Dictionary mapping keeps key access compact through the local boundary, but without repeated adjacent
        // groups a larger accumulator state returns to the staged path for dTLB locality.
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < 100_000; start += 4_096) {
            int size = Math.min(4_096, 100_000 - start);
            long[] keys = new long[size];
            long[] values = new long[size];
            int[] ids = new int[size];
            for (int index = 0; index < size; index++) {
                int id = (index * 37 + 11) % size;
                ids[index] = id;
                long key = start + id;
                long value = key * 7 - 3;
                keys[id] = key;
                values[id] = value;
                long[] state = reference.computeIfAbsent(key, ignored -> new long[2]);
                state[0] += value;
                state[1]++;
            }
            pages.add(TableOperator.Page.values(
                    size,
                    new Vector[] {
                            DictionaryVector.ofTrustedIds(ids, new I64Vector(keys)),
                            DictionaryVector.ofTrustedIds(ids, new I64Vector(values))},
                    Mask.all(size)));
        }
        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountAll()), reference, true);
    }

    @Test
    void fusedHighCardinalityAdjacentRunsRemainFused()
    {
        // Cross the ordinary 64K local-state boundary. Once adjacent reuse is physically proven, switching
        // to staged grouping part-way through duplicates growth work and discards the generated loop's benefit.
        int rows = 200_000;
        int batch = 4_096;
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < rows; start += batch) {
            int size = Math.min(batch, rows - start);
            long[] keys = new long[size];
            long[] values = new long[size];
            for (int index = 0; index < size; index++) {
                long row = start + index;
                long key = row / 2;
                long value = row * 7 - 3;
                keys[index] = key;
                values[index] = value;
                long[] state = reference.computeIfAbsent(key, ignored -> new long[2]);
                state[0] += value;
                state[1]++;
            }
            pages.add(TableOperator.Page.values(size, new Vector[] {new I64Vector(keys), new I64Vector(values)}, Mask.all(size)));
        }
        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountAll()), reference, true);
    }

    @Test
    void fusedRunCountCanDropDuplicateSlotKeysAcrossRehashes()
    {
        String enabled = System.getProperty("nitro.group.idIndexedLong");
        String minimumGroups = System.getProperty("nitro.group.idIndexedLongMinGroups");
        String maximumGroups = System.getProperty("nitro.group.idIndexedLongMaxGroups");
        String capacityMultiplier = System.getProperty("nitro.group.idIndexedLongActivationCapacityMultiplier");
        System.setProperty("nitro.group.idIndexedLong", "true");
        System.setProperty("nitro.group.idIndexedLongMinGroups", "1024");
        System.setProperty("nitro.group.idIndexedLongMaxGroups", "20000");
        System.setProperty("nitro.group.idIndexedLongActivationCapacityMultiplier", "16");
        try {
            int rows = 200_000;
            int batchSize = 4_096;
            List<TableOperator.Page> pages = new ArrayList<>();
            for (int start = 0; start < rows; start += batchSize) {
                int size = Math.min(batchSize, rows - start);
                long[] keys = new long[size];
                for (int index = 0; index < size; index++) {
                    keys[index] = (start + index) / 2;
                }
                pages.add(TableOperator.Page.values(size, new Vector[] {new I64Vector(keys)}, Mask.all(size)));
            }

            Allocator allocator = new Allocator(EngineResources.createDefault());
            Operator operator = new GroupedAggregationOperator(
                    allocator,
                    List.of(0),
                    List.of(new CountAll()),
                    new TableOperator(1, pages));
            int groups = 0;
            try (operator) {
                while (operator.hasNext()) {
                    try (Batch result = operator.next()) {
                        I64Vector counts = (I64Vector) result.output(1).borrow(Stream.VALUES);
                        for (int position : result.borrowMask()) {
                            assertThat(counts.values()[position]).isEqualTo(2);
                            groups++;
                        }
                    }
                }
            }
            assertThat(groups).isEqualTo(rows / 2);
        }
        finally {
            restoreProperty("nitro.group.idIndexedLong", enabled);
            restoreProperty("nitro.group.idIndexedLongMinGroups", minimumGroups);
            restoreProperty("nitro.group.idIndexedLongMaxGroups", maximumGroups);
            restoreProperty("nitro.group.idIndexedLongActivationCapacityMultiplier", capacityMultiplier);
        }
    }

    @Test
    void fusedMappedDirectGroupingFallsBackForWiderKeys()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < 60_000; start += 4_096) {
            int size = Math.min(4_096, 60_000 - start);
            long[] keys = new long[size];
            long[] values = new long[size];
            int[] ids = new int[size];
            for (int index = 0; index < size; index++) {
                int id = (index * 37 + 11) % size;
                ids[index] = id;
                long row = start + id;
                long key = row < 36_000 ? row : (row % 2 == 0 ? row % 20_000 : 200_000 + row);
                long value = row * 7 - 3;
                keys[id] = key;
                values[id] = value;
                long[] state = reference.computeIfAbsent(key, ignored -> new long[2]);
                state[0] += value;
                state[1]++;
            }
            pages.add(TableOperator.Page.values(
                    size,
                    new Vector[] {
                            DictionaryVector.ofTrustedIds(ids, new I64Vector(keys)),
                            DictionaryVector.ofTrustedIds(ids, new I64Vector(values))},
                    Mask.all(size)));
        }
        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountAll()), reference, true);
    }

    @Test
    void fusedHandlesNullValueBatches()
    {
        // Alternate null-free and nullable batches. Both stay fused; SUM skips nulls while COUNT(*)
        // still counts every row.
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

    @Test
    void fusedCountColumnSkipsNullValues()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < 120_000; start += 4_096) {
            int size = Math.min(4_096, 120_000 - start);
            long[] keys = new long[size];
            long[] values = new long[size];
            boolean[] nulls = new boolean[size];
            for (int index = 0; index < size; index++) {
                long row = start + index;
                long key = row % 2_000;
                long value = row * 7 - 3;
                boolean isNull = row % 5 == 0;
                keys[index] = key;
                values[index] = value;
                nulls[index] = isNull;
                long[] state = reference.computeIfAbsent(key, ignored -> new long[2]);
                if (!isNull) {
                    state[0] += value;
                    state[1]++;
                }
            }
            pages.add(new TableOperator.Page(
                    size,
                    new Streams[] {
                            Streams.ofValues(new I64Vector(keys)),
                            Streams.ofValuesAndNulls(new I64Vector(values), new BooleanVector(nulls))},
                    Mask.all(size)));
        }

        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountColumn(1)), reference, true);
    }

    @Test
    void fusedDictionaryInputsMatchReference()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < 80_000; start += 4_096) {
            int size = Math.min(4_096, 80_000 - start);
            int baseSize = size + 17;
            long[] baseKeys = new long[baseSize];
            long[] baseValues = new long[baseSize];
            boolean[] baseNulls = new boolean[baseSize];
            boolean[] logicalNulls = new boolean[size];
            int[] ids = new int[size];
            for (int index = 0; index < baseSize; index++) {
                long row = start + index;
                baseKeys[index] = row % 2_000;
                baseValues[index] = row * 11 - 7;
                baseNulls[index] = row % 7 == 0;
            }
            for (int position = 0; position < size; position++) {
                int id = (position * 37 + 11) % baseSize;
                ids[position] = id;
                logicalNulls[position] = baseNulls[id];
                long key = baseKeys[id];
                long[] state = reference.computeIfAbsent(key, ignored -> new long[2]);
                if (!baseNulls[id]) {
                    state[0] += baseValues[id];
                    state[1]++;
                }
            }
            pages.add(new TableOperator.Page(
                    size,
                    new Streams[] {
                            Streams.ofValues(DictionaryVector.ofTrustedIds(ids, new I64Vector(baseKeys))),
                            Streams.ofValuesAndNulls(
                                    DictionaryVector.ofTrustedIds(ids, new I64Vector(baseValues)),
                                    new BooleanVector(logicalNulls))},
                    Mask.all(size)));
        }

        assertGroupedSumAndCount(pages, List.of(new Sum(1), new CountColumn(1)), reference, true);
    }

    @Test
    void fusedPlainAggregationWritesGroupsForFilteredAccumulator()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < 120_000; start += 4_096) {
            int size = Math.min(4_096, 120_000 - start);
            long[] keys = new long[size];
            long[] values = new long[size];
            boolean[] marker = new boolean[size];
            for (int index = 0; index < size; index++) {
                long row = start + index;
                long key = row % 2_000;
                long value = row * 7 - 3;
                boolean selected = row % 5 == 0;
                keys[index] = key;
                values[index] = value;
                marker[index] = selected;
                long[] state = reference.computeIfAbsent(key, ignored -> new long[2]);
                state[0] += value;
                if (selected) {
                    state[1]++;
                }
            }
            pages.add(TableOperator.Page.values(
                    size,
                    new Vector[] {new I64Vector(keys), new I64Vector(values), new BooleanVector(marker)},
                    Mask.all(size)));
        }

        assertGroupedSumAndCount(
                pages,
                List.of(new Sum(1), new FilteredAccumulator(new CountAll(), 2)),
                reference,
                true);
    }

    @Test
    void fusedGroupingOnlyWritesGroupsForFilteredAccumulator()
    {
        Map<Long, Long> expected = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        for (int start = 0; start < 120_000; start += 4_096) {
            int size = Math.min(4_096, 120_000 - start);
            long[] keys = new long[size];
            boolean[] marker = new boolean[size];
            for (int index = 0; index < size; index++) {
                long row = start + index;
                long key = row % 2_000;
                boolean selected = row % 5 == 0;
                keys[index] = key;
                marker[index] = selected;
                if (selected) {
                    expected.merge(key, 1L, Long::sum);
                }
                else {
                    expected.putIfAbsent(key, 0L);
                }
            }
            pages.add(TableOperator.Page.values(
                    size,
                    new Vector[] {new I64Vector(keys), new BooleanVector(marker)},
                    Mask.all(size)));
        }

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new FilteredAccumulator(new CountAll(), 1)),
                new TableOperator(2, pages));
        Map<Long, Long> actual = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues keys = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    I64Vector counts = (I64Vector) result.output(1).borrow(Stream.VALUES);
                    for (int position : result.borrowMask()) {
                        actual.put(keys.value(position), counts.values()[position]);
                    }
                }
            }
        }
        assertThat(actual).isEqualTo(expected);
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

    private static void restoreProperty(String name, String value)
    {
        if (value == null) {
            System.clearProperty(name);
        }
        else {
            System.setProperty(name, value);
        }
    }

    private static void assertGroupedSumAndCount(
            List<TableOperator.Page> pages,
            List<Accumulator> aggregations,
            Map<Long, long[]> reference,
            boolean checkCount)
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                aggregations,
                new TableOperator(pages.getFirst().columns().length, pages));

        Map<Long, Long> actualSum = new HashMap<>();
        Map<Long, Long> actualCount = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (var result = operator.next()) {
                    Mask mask = result.borrowMask();
                    VectorAccess.LongValues keyColumn = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    I64Vector sumColumn = (I64Vector) result.output(1).borrow(Stream.VALUES);
                    I64Vector countColumn = checkCount ? (I64Vector) result.output(2).borrow(Stream.VALUES) : null;
                    for (int position : mask) {
                        long key = keyColumn.value(position);
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
