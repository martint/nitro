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
import org.weakref.nitro.core.function.aggregation.AggregationExecution;
import org.weakref.nitro.core.function.aggregation.AggregationImplementation;
import org.weakref.nitro.core.function.aggregation.AggregationInput;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationDomain;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdate;
import org.weakref.nitro.core.function.aggregation.GroupedAggregationUpdateTarget;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Utf8Traits;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;
import org.weakref.nitro.operator.aggregation.Accumulator;
import org.weakref.nitro.operator.aggregation.CountAll;
import org.weakref.nitro.operator.aggregation.CountColumn;
import org.weakref.nitro.operator.aggregation.FilteredAccumulator;
import org.weakref.nitro.operator.aggregation.GeneratedRegisteredAggregationUnit;
import org.weakref.nitro.operator.aggregation.MinUtf8;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationProgram;
import org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit;
import org.weakref.nitro.operator.aggregation.StreamAccessor;
import org.weakref.nitro.operator.aggregation.Sum;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit.InputMode.RAW;
import static org.weakref.nitro.operator.aggregation.RegisteredAggregationUnit.OutputMode.FINAL;

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
    void fusedRebindsStateAfterStagedGrowth()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();

        int firstSize = 4_096;
        long[] firstKeys = new long[firstSize];
        int[] firstIds = new int[firstSize];
        for (int index = 0; index < firstSize; index++) {
            firstKeys[index] = index;
            firstIds[index] = index;
            reference.computeIfAbsent((long) index, _ -> new long[2])[0]++;
        }
        pages.add(TableOperator.Page.values(
                firstSize,
                new Vector[] {DictionaryVector.ofTrustedIds(firstIds, new I64Vector(firstKeys))},
                Mask.all(firstSize)));

        // A depth-two dictionary cannot use the generated physical binding, so this batch grows the same logical
        // aggregation state through the staged path.
        int stagedSize = 100_000 - firstSize;
        long[] stagedKeys = new long[stagedSize];
        int[] stagedIds = new int[stagedSize];
        int[] nestedIds = new int[stagedSize];
        for (int index = 0; index < stagedSize; index++) {
            long key = firstSize + index;
            stagedKeys[index] = key;
            stagedIds[index] = index;
            nestedIds[index] = index;
            reference.computeIfAbsent(key, _ -> new long[2])[0]++;
        }
        DictionaryVector nestedKeys = DictionaryVector.ofTrustedIds(
                stagedIds,
                DictionaryVector.ofTrustedIds(nestedIds, new I64Vector(stagedKeys)));
        pages.add(TableOperator.Page.values(stagedSize, new Vector[] {nestedKeys}, Mask.all(stagedSize)));

        // Adjacent reuse makes the generated path eligible again above its ordinary local boundary. It must update
        // the state vector installed by the staged batch, not the smaller vector retained from the first batch.
        int finalSize = 4_096;
        long[] finalKeys = new long[] {99_999};
        int[] finalIds = new int[finalSize];
        reference.get(99_999L)[0] += finalSize;
        pages.add(TableOperator.Page.values(
                finalSize,
                new Vector[] {DictionaryVector.ofTrustedIds(finalIds, new I64Vector(finalKeys))},
                Mask.all(finalSize)));

        assertGroupedSumAndCount(pages, List.of(new CountAll()), reference, false);
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
    void dictionaryDomainCountMatchesAcrossSparseBatchesAndReorderedDomains()
    {
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        long[][] domains = {
                {11, 22, 33, 44},
                {44, 11, 33, 22}};
        for (int batch = 0; batch < domains.length; batch++) {
            int size = 4_096;
            int[] ids = new int[size];
            int[] selected = new int[size - (size + 4) / 5];
            int selectedCount = 0;
            for (int position = 0; position < size; position++) {
                int id = (position * 3 + batch) & 3;
                ids[position] = id;
                if (position % 5 != 0) {
                    selected[selectedCount++] = position;
                    reference.computeIfAbsent(domains[batch][id], ignored -> new long[2])[0]++;
                }
            }
            pages.add(TableOperator.Page.values(
                    size,
                    new Vector[] {DictionaryVector.ofTrustedIds(ids, new I64Vector(domains[batch]))},
                    Mask.sparse(selected, size)));
        }

        assertGroupedSumAndCount(pages, List.of(new CountAll()), reference, false);
    }

    @Test
    void generatedDictionaryDomainCountSupportsLargeSparseDomain()
    {
        int size = 16_384;
        int domainSize = 128;
        int[] ids = new int[size];
        int[] selected = new int[size / 2];
        long[] domain = new long[domainSize];
        Map<Long, long[]> reference = new HashMap<>();
        for (int index = 0; index < domainSize; index++) {
            domain[index] = index * 17L + 11;
        }
        int selectedCount = 0;
        for (int position = 0; position < size; position++) {
            int id = (position * 37 + 5) & (domainSize - 1);
            ids[position] = id;
            if ((position & 1) == 0) {
                selected[selectedCount++] = position;
                reference.computeIfAbsent(domain[id], ignored -> new long[2])[0]++;
            }
        }

        List<TableOperator.Page> pages = List.of(TableOperator.Page.values(
                size,
                new Vector[] {DictionaryVector.ofTrustedIds(ids, new I64Vector(domain))},
                Mask.sparse(selected, size)));

        assertGroupedSumAndCount(pages, List.of(new CountAll()), reference, false);
    }

    @Test
    void dictionaryDomainScratchGrowsAcrossAdmissionBoundary()
    {
        long[] domain = {11, 22, 33, 44};
        Map<Long, long[]> reference = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        // Four rows per physical key admit the generated key-domain path, but not the general path whose extra
        // slot represents SQL NULL. The following batch crosses that boundary. All reusable domain arrays must
        // therefore grow as one invariant even though the first path does not use representative positions.
        for (int size : new int[] {16, 20}) {
            int[] ids = new int[size];
            for (int position = 0; position < size; position++) {
                int id = position & 3;
                ids[position] = id;
                reference.computeIfAbsent(domain[id], ignored -> new long[2])[0]++;
            }
            pages.add(TableOperator.Page.values(
                    size,
                    new Vector[] {DictionaryVector.ofTrustedIds(ids, new I64Vector(domain))},
                    Mask.all(size)));
        }

        assertGroupedSumAndCount(pages, List.of(new CountAll()), reference, false);
    }

    @Test
    void registeredAggregationConsumesSparseDictionaryDomainFrequencies()
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] selected = new int[size - (size + 4) / 5];
        int selectedCount = 0;
        long[] keys = {11, 22, 33, 44};
        Map<Long, Long> expected = new HashMap<>();
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            if (position % 5 != 0) {
                selected[selectedCount++] = position;
                expected.merge(keys[id], 1L, Long::sum);
            }
        }

        WeightedDomainCount implementation = new WeightedDomainCount();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[0]));
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                program,
                new TableOperator(1, List.of(TableOperator.Page.values(
                        size,
                        new Vector[] {DictionaryVector.ofTrustedIds(ids, new I64Vector(keys))},
                        Mask.sparse(selected, size)))));

        Map<Long, Long> actual = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues resultKeys = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultCounts = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.put(resultKeys.value(position), resultCounts.value(position));
                    }
                }
            }
        }

        assertThat(actual).isEqualTo(expected);
        assertThat(implementation.groupedDomainObserved).isTrue();
        assertThat(implementation.logicalRowsObserved).isFalse();
    }

    @Test
    void valueBearingGroupedDomainRequiresExactRowMapping()
    {
        int size = 16_384;
        int[] ids = new int[size];
        for (int position = 0; position < size; position++) {
            ids[position] = position & 3;
        }
        DictionaryVector keys = DictionaryVector.ofTrustedIds(ids, new I64Vector(new long[] {11, 22, 33, 44}));
        DictionaryVector independentlyEncodedValues = DictionaryVector.ofTrustedIds(ids, new I64Vector(new long[] {1, 2, 3, 4}));
        WeightedDomainCount implementation = new WeightedDomainCount(true);
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[] {1}));

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator operator = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        program,
                        new TableOperator(2, List.of(TableOperator.Page.values(
                                size,
                                new Vector[] {keys, independentlyEncodedValues},
                                Mask.all(size)))))) {
            while (operator.hasNext()) {
                operator.next().close();
            }
        }

        assertThat(implementation.groupedDomainObserved).isFalse();
        assertThat(implementation.logicalRowsObserved).isTrue();
    }

    @Test
    void groupedDomainCanExposeSelectedLogicalRepresentatives()
    {
        int size = 16_384;
        int[] ids = new int[size];
        for (int position = 0; position < size; position++) {
            ids[position] = position & 3;
        }
        DictionaryVector keys = DictionaryVector.ofTrustedIds(ids, new I64Vector(new long[] {11, 22, 33, 44}));
        DictionaryVector values = keys.sharedMappingWithValues(new I64Vector(new long[] {1, 2, 3, 4}));
        int[] selected = new int[size - 100];
        for (int index = 0; index < selected.length; index++) {
            selected[index] = index + 100;
        }
        WeightedDomainCount implementation = new WeightedDomainCount(true, true, 100);
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[] {1}));

        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator operator = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        program,
                        new TableOperator(2, List.of(TableOperator.Page.values(
                                size,
                                new Vector[] {keys, values},
                                Mask.sparse(selected, size)))))) {
            while (operator.hasNext()) {
                operator.next().close();
            }
        }

        assertThat(implementation.groupedDomainObserved).isTrue();
        assertThat(implementation.representativesObserved).isTrue();
        assertThat(implementation.logicalRowsObserved).isFalse();
    }

    @Test
    void groupingHashProducerPreservesBinaryDictionaryDomainAggregation()
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] frequencies = new int[4];
        String[] keys = {"alpha", "beta", "gamma", "delta"};
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            frequencies[id]++;
        }

        WeightedDomainCount implementation = new WeightedDomainCount();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                        new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[0]))
                .withGroupingHashOutput(new GroupingHashOutput(
                        "test-dictionary-domain-hash-v1",
                        new Field(Schema.unspecified(1).field(0).type(), false)));
        DictionaryVector dictionary = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                utf8(keys),
                frequencies);
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator operator = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        program,
                        new TableOperator(1, List.of(TableOperator.Page.values(
                                size,
                                new Vector[] {dictionary},
                                Mask.all(size)))))) {
            Map<String, Long> counts = new HashMap<>();
            Map<String, Long> hashes = new HashMap<>();
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    Vector resultKeys = result.output(0).borrow(Stream.VALUES);
                    VectorAccess.LongValues resultCounts = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultHashes = VectorAccess.longValues(result.output(2).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        String key = utf8(resultKeys, position);
                        counts.put(key, resultCounts.value(position));
                        hashes.put(key, resultHashes.value(position));
                    }
                }
            }

            assertThat(counts).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "alpha", (long) frequencies[0],
                    "beta", (long) frequencies[1],
                    "gamma", (long) frequencies[2],
                    "delta", (long) frequencies[3]));
            Vector domain = dictionary.values();
            assertThat(hashes).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "alpha", (long) OperatorVectorSupport.binaryHash(domain, 0),
                    "beta", (long) OperatorVectorSupport.binaryHash(domain, 1),
                    "gamma", (long) OperatorVectorSupport.binaryHash(domain, 2),
                    "delta", (long) OperatorVectorSupport.binaryHash(domain, 3)));
        }
        assertThat(implementation.groupedDomainObserved).isTrue();
        assertThat(implementation.logicalRowsObserved).isFalse();
    }

    @Test
    void registeredAggregationConsumesSharedCompositeDictionaryDomain()
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] frequencies = new int[4];
        long[] firstKeys = {1, 1, 2, 2};
        long[] secondKeys = {10, 20, 10, 20};
        long[] thirdKeys = {100, 200, 300, 400};
        Map<String, Long> expected = new HashMap<>();
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            frequencies[id]++;
            expected.merge(firstKeys[id] + ":" + secondKeys[id] + ":" + thirdKeys[id], 1L, Long::sum);
        }

        WeightedDomainCount implementation = new WeightedDomainCount();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[0]));
        Allocator allocator = new Allocator(EngineResources.createDefault());
        DictionaryVector firstDictionary = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                new I64Vector(firstKeys),
                frequencies);
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1, 2),
                program,
                new TableOperator(3, List.of(TableOperator.Page.values(
                        size,
                        new Vector[] {
                                firstDictionary,
                                firstDictionary.sharedMappingWithValues(new I64Vector(secondKeys)),
                                firstDictionary.sharedMappingWithValues(new I64Vector(thirdKeys))},
                        Mask.all(size)))));

        Map<String, Long> actual = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues first = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues second = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    VectorAccess.LongValues third = VectorAccess.longValues(result.output(2).borrow(Stream.VALUES));
                    VectorAccess.LongValues counts = VectorAccess.longValues(result.output(3).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.put(
                                first.value(position) + ":" + second.value(position) + ":" + third.value(position),
                                counts.value(position));
                    }
                }
            }
        }

        assertThat(actual).isEqualTo(expected);
        assertThat(implementation.groupedDomainObserved).isTrue();
        assertThat(implementation.logicalRowsObserved).isFalse();
    }

    @Test
    void groupingHashProducerPreservesArbitraryAritySharedDictionaryDomain()
    {
        assertGroupingHashContractPreservesArbitraryAritySharedDictionaryDomain(true);
    }

    @Test
    void authoritativeHashConsumerPreservesArbitraryAritySharedDictionaryDomain()
    {
        assertGroupingHashContractPreservesArbitraryAritySharedDictionaryDomain(false);
    }

    private static void assertGroupingHashContractPreservesArbitraryAritySharedDictionaryDomain(boolean producer)
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] frequencies = new int[4];
        long[] firstKeys = {1, 1, 2, 2};
        long[] secondKeys = {10, 20, 10, 20};
        long[] thirdKeys = {100, 200, 300, 400};
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            frequencies[id]++;
        }

        WeightedDomainCount implementation = new WeightedDomainCount();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[0]));
        program = producer
                ? program.withGroupingHashOutput(new GroupingHashOutput(
                        "test-shared-composite-domain-hash-v1",
                        new Field(Schema.unspecified(1).field(0).type(), false)))
                : program.withAuthoritativeHashChannel(new AuthoritativeHashChannel(
                        "test-shared-composite-domain-hash-v1",
                        3,
                        true));
        DictionaryVector firstDictionary = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                new I64Vector(firstKeys),
                frequencies);
        long[] domainHashes = new long[firstKeys.length];
        for (int domain = 0; domain < domainHashes.length; domain++) {
            domainHashes[domain] = 31L * (31L * Long.hashCode(firstKeys[domain]) + Long.hashCode(secondKeys[domain])) +
                    Long.hashCode(thirdKeys[domain]);
        }
        Vector[] inputs = {
                firstDictionary,
                firstDictionary.sharedMappingWithValues(new I64Vector(secondKeys)),
                firstDictionary.sharedMappingWithValues(new I64Vector(thirdKeys))};
        if (!producer) {
            inputs = Arrays.copyOf(inputs, 4);
            inputs[3] = firstDictionary.sharedMappingWithValues(new I64Vector(domainHashes));
        }
        MutableAggregationPhaseMetrics phaseMetrics = new MutableAggregationPhaseMetrics();
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator operator = new GroupedAggregationOperator(
                        allocator,
                        List.of(0, 1, 2),
                        List.of(0, 1, 2),
                        program,
                        new TableOperator(inputs.length, List.of(TableOperator.Page.values(
                                size,
                                inputs,
                                Mask.all(size)))),
                        resources.operatorResources(),
                        resources.operatorResources().grouping(),
                        new Allocator.Context("grouping-hash-domain-metrics"),
                        phaseMetrics)) {
            Map<String, Long> counts = new HashMap<>();
            Map<String, Long> hashes = new HashMap<>();
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues first = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues second = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    VectorAccess.LongValues third = VectorAccess.longValues(result.output(2).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultCounts = VectorAccess.longValues(result.output(3).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultHashes = VectorAccess.longValues(result.output(4).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        String key = first.value(position) + ":" + second.value(position) + ":" + third.value(position);
                        counts.put(key, resultCounts.value(position));
                        hashes.put(key, resultHashes.value(position));
                    }
                }
            }

            for (int domain = 0; domain < firstKeys.length; domain++) {
                String key = firstKeys[domain] + ":" + secondKeys[domain] + ":" + thirdKeys[domain];
                assertThat(counts.get(key)).isEqualTo(frequencies[domain]);
                assertThat(hashes.get(key)).isEqualTo(domainHashes[domain]);
            }
        }
        assertThat(implementation.groupedDomainObserved).isTrue();
        assertThat(implementation.logicalRowsObserved).isFalse();
        AggregationPhaseMetrics metrics = phaseMetrics.snapshot();
        assertThat(metrics.encodedKeyDomainBatches()).isOne();
        assertThat(metrics.authoritativeHashDomainBatches()).isEqualTo(producer ? 0 : 1);
        assertThat(metrics.computedHashDomainBatches()).isZero();
        assertThat(metrics.authoritativeHashRowBatches()).isZero();
        assertThat(metrics.computedHashRowBatches()).isZero();
        assertThat(metrics.computedHashOutputBatches()).isEqualTo(producer ? 1 : 0);
    }

    @Test
    void registeredAggregationConsumesCompactDictionarySelectionFrequencies()
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] frequencies = new int[4];
        long[] keys = {11, 22, 33, 44};
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            frequencies[id]++;
        }
        DictionaryVector dictionary = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                new I64Vector(keys),
                frequencies);
        Mask selected = Mask.all(size);
        selected.retainDictionaryComparison(dictionary, new boolean[] {false, true, false, true});

        WeightedDomainCount implementation = new WeightedDomainCount();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[0]));
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                program,
                new TableOperator(1, List.of(TableOperator.Page.values(size, new Vector[] {dictionary}, selected))));

        Map<Long, Long> actual = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues resultKeys = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultCounts = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.put(resultKeys.value(position), resultCounts.value(position));
                    }
                }
            }
        }

        assertThat(actual).containsExactlyInAnyOrderEntriesOf(Map.of(22L, (long) frequencies[1], 44L, (long) frequencies[3]));
        assertThat(implementation.groupedDomainObserved).isTrue();
        assertThat(implementation.logicalRowsObserved).isFalse();
    }

    @Test
    void filteredRegisteredAggregationPreservesEncodedGroups()
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] frequencies = new int[4];
        long[] keys = {0, 1, 0, 1};
        long[] values = {10, 20, 30, 40};
        boolean[] selectedDomain = {false, true, false, true};
        Map<Long, Long> expected = new HashMap<>();
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            frequencies[id]++;
            if (selectedDomain[id]) {
                expected.merge(keys[id], values[id], Long::sum);
            }
            else {
                expected.putIfAbsent(keys[id], 0L);
            }
        }

        DictionaryVector keyDictionary = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                new I64Vector(keys),
                frequencies);
        EncodedGeneratedSum implementation = new EncodedGeneratedSum();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[] {1}, 2));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator operator = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        program,
                        new TableOperator(3, List.of(TableOperator.Page.values(
                                size,
                                new Vector[] {
                                        keyDictionary,
                                        keyDictionary.sharedMappingWithValues(new I64Vector(values)),
                                        keyDictionary.sharedMappingWithValues(new BooleanVector(selectedDomain))},
                                Mask.all(size)))))) {
            Map<Long, Long> actual = new HashMap<>();
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues resultKeys = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultSums = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.put(resultKeys.value(position), resultSums.value(position));
                    }
                }
            }
            assertThat(actual).isEqualTo(expected);
        }
        assertThat(implementation.encodedGroupsObserved).isTrue();
    }

    @Test
    void filteredRegisteredAggregationFallsBackWhenEncodedGroupsAreUnsupported()
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] frequencies = new int[4];
        long[] keys = {0, 1, 0, 1};
        boolean[] selectedDomain = {false, true, false, true};
        Map<Long, Long> expected = new HashMap<>();
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            frequencies[id]++;
            expected.putIfAbsent(keys[id], 0L);
            if (selectedDomain[id]) {
                expected.merge(keys[id], 1L, Long::sum);
            }
        }

        DictionaryVector keyDictionary = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                new I64Vector(keys),
                frequencies);
        WeightedDomainCount implementation = new WeightedDomainCount();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[0], 1));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator operator = new GroupedAggregationOperator(
                        allocator,
                        List.of(0),
                        program,
                        new TableOperator(2, List.of(TableOperator.Page.values(
                                size,
                                new Vector[] {
                                        keyDictionary,
                                        keyDictionary.sharedMappingWithValues(new BooleanVector(selectedDomain))},
                                Mask.all(size)))))) {
            Map<Long, Long> actual = new HashMap<>();
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues resultKeys = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultCounts = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.put(resultKeys.value(position), resultCounts.value(position));
                    }
                }
            }
            assertThat(actual).isEqualTo(expected);
        }
        assertThat(implementation.logicalRowsObserved).isTrue();
        assertThat(implementation.groupedDomainObserved).isFalse();
    }

    @Test
    void filteredRegisteredAggregationPreservesSharedCompositeEncodedGroups()
    {
        int size = 16_384;
        int domainSize = 128;
        int[] ids = new int[size];
        int[] frequencies = new int[domainSize];
        long[] firstKeys = new long[domainSize];
        long[] secondKeys = new long[domainSize];
        long[] values = new long[domainSize];
        boolean[] selectedDomain = new boolean[domainSize];
        for (int domain = 0; domain < domainSize; domain++) {
            firstKeys[domain] = domain / 16;
            secondKeys[domain] = domain % 16;
            values[domain] = domain + 1;
            selectedDomain[domain] = (domain & 1) != 0;
        }
        Map<String, Long> expected = new HashMap<>();
        for (int position = 0; position < size; position++) {
            int id = (position * 37 + 5) & (domainSize - 1);
            ids[position] = id;
            frequencies[id]++;
            String key = firstKeys[id] + ":" + secondKeys[id];
            expected.putIfAbsent(key, 0L);
            if (selectedDomain[id]) {
                expected.merge(key, values[id], Long::sum);
            }
        }

        DictionaryVector firstDictionary = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                new I64Vector(firstKeys),
                frequencies);
        EncodedGeneratedSum implementation = new EncodedGeneratedSum();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new RegisteredAggregationUnit(implementation, RAW, FINAL, new int[] {2}, 3));
        try (EngineResources resources = EngineResources.createDefault();
                Allocator allocator = new Allocator(resources);
                Operator operator = new GroupedAggregationOperator(
                        allocator,
                        List.of(0, 1),
                        program,
                        new TableOperator(4, List.of(TableOperator.Page.values(
                                size,
                                new Vector[] {
                                        firstDictionary,
                                        firstDictionary.sharedMappingWithValues(new I64Vector(secondKeys)),
                                        firstDictionary.sharedMappingWithValues(new I64Vector(values)),
                                        firstDictionary.sharedMappingWithValues(new BooleanVector(selectedDomain))},
                                Mask.all(size)))))) {
            Map<String, Long> actual = new HashMap<>();
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues first = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues second = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    VectorAccess.LongValues sums = VectorAccess.longValues(result.output(2).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.put(first.value(position) + ":" + second.value(position), sums.value(position));
                    }
                }
            }
            assertThat(actual).isEqualTo(expected);
        }
        assertThat(implementation.encodedGroupsObserved).isTrue();
    }

    @Test
    void allNullAggregationInputPreservesCompactDictionarySelection()
    {
        int size = 16_384;
        int[] ids = new int[size];
        int[] frequencies = new int[4];
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            frequencies[id]++;
        }
        DictionaryVector keys = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                new I64Vector(new long[] {11, 22, 33, 44}),
                frequencies);
        Mask selected = Mask.all(size);
        selected.retainDictionaryComparison(keys, new boolean[] {false, true, false, true});
        Mask operatorMask = selected.copy();
        Batch input = new Batch(
                operatorMask,
                Output.of(Streams.ofValues(keys)),
                Output.of(Streams.ofValues(keys.sharedMappingWithValues(new I64Vector(new long[] {1, 1, 1, 1})))
                        .with(Stream.NULLS, new RleVector(new int[] {size}, new BooleanVector(new boolean[] {true})))));
        Operator source = new Operator()
        {
            private boolean available = true;

            @Override
            public int outputCount()
            {
                return 2;
            }

            @Override
            public boolean hasNext()
            {
                return available;
            }

            @Override
            public Batch next()
            {
                available = false;
                return input;
            }

            @Override
            public void constrain(Mask mask) {}

            @Override
            public void close() {}
        };

        Allocator allocator = new Allocator(EngineResources.createDefault());
        try (Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountColumn(1)),
                source)) {
            while (operator.hasNext()) {
                try (Batch ignored = operator.next()) {
                    assertThat(ignored.borrowMask().count()).isEqualTo(2);
                }
            }
        }

        assertThat(operatorMask.dictionaryDomainSelection(keys)).isNotNull();
    }

    @Test
    void binaryDictionaryDomainCountMatchesAcrossSparseBatchesAndReorderedDomains()
    {
        Map<String, Long> expected = new HashMap<>();
        List<TableOperator.Page> pages = new ArrayList<>();
        String[][] domains = {
                {"alpha", "beta", "gamma", "delta"},
                {"delta", "alpha", "gamma", "beta"}};
        for (int batch = 0; batch < domains.length; batch++) {
            int size = 4_096;
            int[] ids = new int[size];
            int[] selected = new int[size - (size + 4) / 5];
            int selectedCount = 0;
            for (int position = 0; position < size; position++) {
                int id = (position * 3 + batch) & 3;
                ids[position] = id;
                if (position % 5 != 0) {
                    selected[selectedCount++] = position;
                    expected.merge(domains[batch][id], 1L, Long::sum);
                }
            }
            pages.add(TableOperator.Page.values(
                    size,
                    new Vector[] {DictionaryVector.ofTrustedIds(ids, utf8(domains[batch]))},
                    Mask.sparse(selected, size)));
        }

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new CountAll()),
                new TableOperator(1, pages));
        Map<String, Long> actual = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    Vector keys = result.output(0).borrow(Stream.VALUES);
                    I64Vector counts = (I64Vector) result.output(1).borrow(Stream.VALUES);
                    for (int position : result.borrowMask()) {
                        actual.put(utf8(keys, position), counts.values()[position]);
                    }
                }
            }
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void binaryDictionaryDomainConsumesAlignedInputMapping()
    {
        int size = 16_384;
        int[] ids = new int[size];
        boolean[] logicalNulls = new boolean[size];
        String[] keys = {"alpha", "beta", "gamma", "delta"};
        long[] domainValues = {11, 22, 33, 44};
        Map<String, long[]> expected = new HashMap<>();
        for (int position = 0; position < size; position++) {
            int id = (position * 3 + 1) & 3;
            ids[position] = id;
            logicalNulls[position] = position % 11 == 0;
            if (!logicalNulls[position]) {
                long[] state = expected.computeIfAbsent(keys[id], _ -> new long[2]);
                state[0] += domainValues[id];
                state[1]++;
            }
        }
        DictionaryVector keyDictionary = DictionaryVector.wrapNested(ids, size, utf8(keys));
        DictionaryVector valueDictionary = keyDictionary.sharedMappingWithValues(new I64Vector(domainValues));
        TableOperator.Page page = new TableOperator.Page(
                size,
                new Streams[] {
                        Streams.ofValues(keyDictionary),
                        Streams.ofValuesAndNulls(valueDictionary, new BooleanVector(logicalNulls))},
                Mask.all(size));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(new Sum(1), new CountColumn(1)),
                new TableOperator(2, List.of(page)));
        Map<String, long[]> actual = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    Vector resultKeys = result.output(0).borrow(Stream.VALUES);
                    I64Vector sums = (I64Vector) result.output(1).borrow(Stream.VALUES);
                    I64Vector counts = (I64Vector) result.output(2).borrow(Stream.VALUES);
                    for (int position : result.borrowMask()) {
                        actual.put(utf8(resultKeys, position), new long[] {
                                sums.values()[position], counts.values()[position]});
                    }
                }
            }
        }
        assertThat(actual).containsOnlyKeys(expected.keySet());
        expected.forEach((key, value) -> assertThat(actual.get(key)).containsExactly(value));
    }

    @Test
    void registeredAggregationReceivesEncodedGroupIds()
    {
        int size = 16_384;
        int[] ids = new int[size];
        String[] values = {"delta", "alpha", "charlie", "bravo"};
        for (int position = 0; position < size; position++) {
            ids[position] = (position * 3 + 1) & 3;
        }
        DictionaryVector keys = DictionaryVector.ofTrustedIdsWithDomainFrequencies(
                ids,
                size,
                utf8(values),
                new int[] {size / 4, size / 4, size / 4, size / 4});
        DictionaryVector input = keys.sharedMappingWithValues(utf8(values));
        EncodedMinUtf8 minimum = new EncodedMinUtf8(1);

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(minimum),
                new TableOperator(2, List.of(TableOperator.Page.values(
                        size,
                        new Vector[] {keys, input},
                        Mask.all(size)))));
        try (operator) {
            while (operator.hasNext()) {
                try (Batch ignored = operator.next()) {
                    assertThat(ignored.borrowMask().count()).isEqualTo(values.length);
                }
            }
        }
        assertThat(minimum.encodedGroupsObserved).isTrue();
        assertThat(minimum.encodedGroupFrequenciesObserved).isTrue();
    }

    @Test
    void generatedRegisteredAggregationFallsBackToEncodedProviderForIndependentMappings()
    {
        int size = 16_384;
        int[] keyIds = new int[size];
        int[] valueIds = new int[size];
        Map<Long, Long> expected = new HashMap<>();
        long[] keyDomain = {10, 20};
        long[] valueDomain = {3, 5, 7, 11};
        for (int position = 0; position < size; position++) {
            keyIds[position] = position & 1;
            valueIds[position] = (position * 3 + 1) & 3;
            expected.merge(keyDomain[keyIds[position]], valueDomain[valueIds[position]], Long::sum);
        }
        DictionaryVector keys = DictionaryVector.wrapNested(keyIds, size, new I64Vector(keyDomain));
        DictionaryVector values = DictionaryVector.wrapNested(valueIds, size, new I64Vector(valueDomain));
        EncodedGeneratedSum implementation = new EncodedGeneratedSum();
        PhysicalAggregationProgram program = PhysicalAggregationProgram.singleUnit(
                new GeneratedRegisteredAggregationUnit(
                        implementation,
                        RAW,
                        FINAL,
                        new int[] {1},
                        -1,
                        GroupedAggregationUpdate.inputValue(1, encodedGeneratedSumTarget())));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        Operator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                program,
                new TableOperator(2, List.of(TableOperator.Page.values(
                        size,
                        new Vector[] {keys, values},
                        Mask.all(size)))));
        Map<Long, Long> actual = new HashMap<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues resultKeys = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    VectorAccess.LongValues resultSums = VectorAccess.longValues(result.output(1).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.put(resultKeys.value(position), resultSums.value(position));
                    }
                }
            }
        }

        assertThat(actual).isEqualTo(expected);
        assertThat(implementation.encodedGroupsObserved).isTrue();
    }

    @Test
    void keyOnlyDictionaryGroupingDiscardsLogicalGroupIds()
    {
        int size = 100_000;
        int[] ids = new int[size];
        for (int position = 0; position < size; position++) {
            ids[position] = position & 3;
        }
        List<TableOperator.Page> pages = List.of(TableOperator.Page.values(
                size,
                new Vector[] {DictionaryVector.ofTrustedIds(ids, new I64Vector(new long[] {44, 11, 33, 22}))},
                Mask.all(size)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        GroupedAggregationOperator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0),
                List.of(),
                new TableOperator(1, pages));
        List<Long> actual = new ArrayList<>();
        try (operator) {
            while (operator.hasNext()) {
                try (Batch result = operator.next()) {
                    VectorAccess.LongValues values = VectorAccess.longValues(result.output(0).borrow(Stream.VALUES));
                    for (int position : result.borrowMask()) {
                        actual.add(values.value(position));
                    }
                }
            }
        }
        assertThat(actual).containsExactlyInAnyOrder(11L, 22L, 33L, 44L);
        assertThat(operator.discardsInlineGroupIds()).isTrue();
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

    @Test
    void groupingOnlyFullWidthPairKeepsGeneratedRepresentation()
    {
        int size = 1 << 10;
        long[] first = new long[size];
        long[] second = new long[size];
        for (int position = 0; position < size; position++) {
            first[position] = (1L << 40) + position;
            second[position] = position * 3L;
        }
        List<TableOperator.Page> pages = List.of(TableOperator.Page.values(
                size,
                new Vector[] {new I64Vector(first), new I64Vector(second)},
                Mask.all(size)));

        Allocator allocator = new Allocator(EngineResources.createDefault());
        GroupedAggregationOperator operator = new GroupedAggregationOperator(
                allocator,
                List.of(0, 1),
                List.of(),
                new TableOperator(2, pages));
        try (operator) {
            assertThat(operator.hasNext()).isTrue();
            try (Batch result = operator.next()) {
                assertThat(result.borrowMask().count()).isEqualTo(size);
            }
            assertThat(operator.usesPackedFlatIdentitySlots()).isFalse();
            assertThat(operator.discardsInlineGroupIds()).isTrue();
        }
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

    private static BinaryVector utf8(String... values)
    {
        int totalBytes = Arrays.stream(values).mapToInt(String::length).sum();
        BinaryVector vector = new BinaryVector(values.length, totalBytes);
        vector.addTrait(Utf8Traits.UTF8_VALID);
        vector.addTrait(Utf8Traits.ASCII_ONLY);
        for (int index = 0; index < values.length; index++) {
            vector.setBytes(index, values[index].getBytes(StandardCharsets.UTF_8));
        }
        return vector;
    }

    private static String utf8(Vector vector, int position)
    {
        return switch (vector) {
            case BinaryVector binary -> new String(binary.copyBytes(position), StandardCharsets.UTF_8);
            case DictionaryVector dictionary -> utf8(dictionary.values(), dictionary.ids()[position]);
            default -> throw new IllegalArgumentException("Expected binary vector but found " + vector.getClass().getSimpleName());
        };
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

    private static final class EncodedMinUtf8
            extends MinUtf8
    {
        private boolean encodedGroupsObserved;
        private boolean encodedGroupFrequenciesObserved;

        private EncodedMinUtf8(int inputColumn)
        {
            super(inputColumn);
        }

        @Override
        public boolean supportsEncodedGroupedInput()
        {
            return true;
        }

        @Override
        public void accumulate(Streams state, Vector groups, Mask mask, StreamAccessor streams)
        {
            encodedGroupsObserved |= groups instanceof DictionaryVector;
            encodedGroupFrequenciesObserved |= groups instanceof DictionaryVector dictionary && dictionary.hasDomainFrequencies();
            VectorAccess.LongValues groupValues = VectorAccess.longValues(groups);
            I64Vector flatGroups = new I64Vector(groups.length());
            for (int position : mask) {
                flatGroups.values()[position] = groupValues.value(position);
            }
            super.accumulate(state, flatGroups, mask, streams);
        }
    }

    private static final class WeightedDomainCount
            implements AggregationImplementation
    {
        private final boolean requireInputMapping;
        private final boolean requireRepresentatives;
        private final int minimumRepresentative;
        private boolean groupedDomainObserved;
        private boolean logicalRowsObserved;
        private boolean representativesObserved;

        private WeightedDomainCount()
        {
            this(false);
        }

        private WeightedDomainCount(boolean requireInputMapping)
        {
            this(requireInputMapping, false, 0);
        }

        private WeightedDomainCount(boolean requireInputMapping, boolean requireRepresentatives, int minimumRepresentative)
        {
            this.requireInputMapping = requireInputMapping;
            this.requireRepresentatives = requireRepresentatives;
            this.minimumRepresentative = minimumRepresentative;
        }

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            return new long[groups];
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            return Arrays.copyOf((long[]) state, groups);
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            Arrays.fill((long[]) state, offset, offset + length, 0);
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            logicalRowsObserved = true;
            ((long[]) state)[group] += mask.count();
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            logicalRowsObserved = true;
            VectorAccess.LongValues groupIds = VectorAccess.longValues(groups);
            for (int position : mask) {
                ((long[]) state)[(int) groupIds.value(position)]++;
            }
        }

        @Override
        public boolean supportsRawGroupedDomainInput(AggregationInput input)
        {
            return true;
        }

        @Override
        public boolean supportsRawGroupedDomainInput(DictionaryVector rowMapping, AggregationInput input)
        {
            return !requireInputMapping ||
                    (input.stream(0, Stream.VALUES) instanceof DictionaryVector dictionary &&
                            rowMapping.hasSameRowMapping(dictionary));
        }

        @Override
        public boolean supportsEncodedGroupedInput()
        {
            return requireInputMapping;
        }

        @Override
        public boolean requiresRawGroupedDomainRepresentatives(DictionaryVector rowMapping, AggregationInput input)
        {
            return requireRepresentatives;
        }

        @Override
        public void addRawGroupedDomainInput(Object state, GroupedAggregationDomain domain, AggregationInput input)
        {
            groupedDomainObserved = true;
            VectorAccess.LongValues groupIds = VectorAccess.longValues(domain.groups());
            for (int physical = 0; physical < domain.size(); physical++) {
                if (requireRepresentatives && domain.frequency(physical) != 0) {
                    int representative = domain.representative(physical);
                    assertThat(representative).isGreaterThanOrEqualTo(minimumRepresentative);
                    assertThat(domain.rowMapping().ids()[representative]).isEqualTo(physical);
                    representativesObserved = true;
                }
                ((long[]) state)[(int) groupIds.value(physical)] += domain.frequency(physical);
            }
        }

        @Override
        public void addIntermediate(Object state, int group, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams intermediate(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            return result(maxGroup, state, existing, allocator, allocationContext);
        }

        @Override
        public Streams result(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            I64Vector output = allocator.allocate(allocationContext, I64Vector.class, maxGroup + 1, I64Vector::new);
            System.arraycopy(state, 0, output.values(), 0, maxGroup + 1);
            return Streams.ofValues(output);
        }
    }

    private static final class EncodedGeneratedSum
            implements AggregationImplementation
    {
        private boolean encodedGroupsObserved;

        @Override
        public Object allocate(AggregationExecution execution, int groups)
        {
            return new EncodedGeneratedSumState(groups);
        }

        @Override
        public Object grow(Allocator allocator, Allocator.Context allocationContext, Object state, int groups)
        {
            EncodedGeneratedSumState previous = (EncodedGeneratedSumState) state;
            return new EncodedGeneratedSumState(Arrays.copyOf(previous.sums, groups));
        }

        @Override
        public void initialize(Object state, int offset, int length)
        {
            Arrays.fill(((EncodedGeneratedSumState) state).sums, offset, offset + length, 0);
        }

        @Override
        public void addRawInput(Object state, int group, Mask mask, AggregationInput input)
        {
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
            for (int position : mask) {
                ((EncodedGeneratedSumState) state).update(group, values.value(position));
            }
        }

        @Override
        public void addRawInput(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            encodedGroupsObserved |= groups instanceof DictionaryVector;
            VectorAccess.LongValues groupValues = VectorAccess.longValues(groups);
            VectorAccess.LongValues values = VectorAccess.longValues(input.stream(0, Stream.VALUES));
            for (int position : mask) {
                ((EncodedGeneratedSumState) state).update((int) groupValues.value(position), values.value(position));
            }
        }

        @Override
        public boolean supportsEncodedGroupedInput()
        {
            return true;
        }

        @Override
        public void addIntermediate(Object state, int group, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(Object state, Vector groups, Mask mask, AggregationInput input)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Streams intermediate(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            return result(maxGroup, state, existing, allocator, allocationContext);
        }

        @Override
        public Streams result(
                int maxGroup,
                Object state,
                Streams existing,
                Allocator allocator,
                Allocator.Context allocationContext)
        {
            I64Vector output = allocator.allocate(allocationContext, I64Vector.class, maxGroup + 1, I64Vector::new);
            System.arraycopy(((EncodedGeneratedSumState) state).sums, 0, output.values(), 0, maxGroup + 1);
            return Streams.ofValues(output);
        }
    }

    private static GroupedAggregationUpdateTarget encodedGeneratedSumTarget()
    {
        try {
            return new GroupedAggregationUpdateTarget(MethodHandles.lookup().findVirtual(
                    EncodedGeneratedSumState.class,
                    "update",
                    MethodType.methodType(void.class, int.class, long.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final class EncodedGeneratedSumState
    {
        private final long[] sums;

        private EncodedGeneratedSumState(int groups)
        {
            this(new long[groups]);
        }

        private EncodedGeneratedSumState(long[] sums)
        {
            this.sums = sums;
        }

        public void update(int group, long value)
        {
            sums[group] += value;
        }
    }
}
