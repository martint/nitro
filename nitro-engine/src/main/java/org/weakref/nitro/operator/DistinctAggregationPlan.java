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

import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.aggregation.PhysicalAggregationUnit;
import org.weakref.nitro.operator.aggregation.StreamAccessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class DistinctAggregationPlan
{
    private final int[] plainAggregationIndexes;
    private final int[] filteredAggregationIndexes;
    private final Group[] distinctAggregationGroups;

    private DistinctAggregationPlan(int[] plainAggregationIndexes, int[] filteredAggregationIndexes, Group[] distinctAggregationGroups)
    {
        this.plainAggregationIndexes = plainAggregationIndexes;
        this.filteredAggregationIndexes = filteredAggregationIndexes;
        this.distinctAggregationGroups = distinctAggregationGroups;
    }

    static DistinctAggregationPlan plan(
            PhysicalAggregationUnit[] aggregations,
            boolean grouped,
            boolean groupPartitionedLongDistinct,
            Schema sourceSchema)
    {
        List<Integer> plainAggregationIndexes = new ArrayList<>();
        List<Integer> filteredAggregationIndexes = new ArrayList<>();
        Map<Signature, List<Integer>> aggregationIndexesBySignature = new LinkedHashMap<>();
        for (int aggregationIndex = 0; aggregationIndex < aggregations.length; aggregationIndex++) {
            int[] distinctInputColumns = aggregations[aggregationIndex].distinctInputColumns();
            if (distinctInputColumns == null || distinctInputColumns.length == 0) {
                if (aggregations[aggregationIndex].filterInputColumn() < 0) {
                    plainAggregationIndexes.add(aggregationIndex);
                }
                else {
                    filteredAggregationIndexes.add(aggregationIndex);
                }
                continue;
            }
            aggregationIndexesBySignature.computeIfAbsent(
                            new Signature(distinctInputColumns, aggregations[aggregationIndex].filterInputColumn()),
                            _ -> new ArrayList<>())
                    .add(aggregationIndex);
        }

        Group[] distinctAggregationGroups = aggregationIndexesBySignature.entrySet().stream()
                .map(entry -> new Group(
                        entry.getKey().inputColumns(),
                        entry.getKey().filterInputColumn(),
                        entry.getValue().stream().mapToInt(Integer::intValue).toArray(),
                        grouped,
                        groupPartitionedLongDistinct,
                        distinctTypes(sourceSchema, entry.getKey().inputColumns())))
                .toArray(Group[]::new);
        return new DistinctAggregationPlan(
                plainAggregationIndexes.stream().mapToInt(Integer::intValue).toArray(),
                filteredAggregationIndexes.stream().mapToInt(Integer::intValue).toArray(),
                distinctAggregationGroups);
    }

    int[] plainAggregationIndexes()
    {
        return plainAggregationIndexes;
    }

    int[] filteredAggregationIndexes()
    {
        return filteredAggregationIndexes;
    }

    Group[] distinctAggregationGroups()
    {
        return distinctAggregationGroups;
    }

    void releaseBuffers()
    {
        for (Group group : distinctAggregationGroups) {
            group.releaseBuffers();
        }
    }

    private static List<TypeBinding> distinctTypes(Schema schema, int[] inputColumns)
    {
        for (int column : inputColumns) {
            if (column < 0 || column >= schema.size()) {
                return List.of();
            }
        }
        return Arrays.stream(inputColumns)
                .mapToObj(column -> schema.field(column).type())
                .toList();
    }

    private record Signature(int[] inputColumns, int filterInputColumn)
    {
        private Signature
        {
            inputColumns = inputColumns.clone();
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof Signature signature &&
                    filterInputColumn == signature.filterInputColumn &&
                    Arrays.equals(inputColumns, signature.inputColumns);
        }

        @Override
        public int hashCode()
        {
            return 31 * Arrays.hashCode(inputColumns) + filterInputColumn;
        }
    }

    static final class Group
    {
        private static final int[] EMPTY_POSITIONS = new int[0];
        private final int[] inputColumns;
        private final int filterInputColumn;
        private final int[] aggregationIndexes;
        private final boolean grouped;
        private final boolean groupPartitionedLongDistinct;
        private final List<TypeBinding> inputTypes;
        private final Vector[] values;
        private final Vector[] nulls;
        private DistinctKeySet distinctKeySet;
        private PrimitiveArrayPool arrayPool;
        private int[] distinctPositions = EMPTY_POSITIONS;

        private Group(
                int[] inputColumns,
                int filterInputColumn,
                int[] aggregationIndexes,
                boolean grouped,
                boolean groupPartitionedLongDistinct,
                List<TypeBinding> inputTypes)
        {
            this.inputColumns = inputColumns.clone();
            this.filterInputColumn = filterInputColumn;
            this.aggregationIndexes = aggregationIndexes;
            this.grouped = grouped;
            this.groupPartitionedLongDistinct = groupPartitionedLongDistinct;
            this.inputTypes = List.copyOf(inputTypes);
            this.values = new Vector[inputColumns.length + (grouped ? 1 : 0)];
            this.nulls = new Vector[this.values.length];
        }

        int[] aggregationIndexes()
        {
            return aggregationIndexes;
        }

        int filterInputColumn()
        {
            return filterInputColumn;
        }

        Mask select(
                I64Vector groups,
                Mask mask,
                StreamAccessor streamAccessor,
                int groupCount,
                Allocator allocator,
                Allocator.Context allocationContext,
                OperatorCodeGenerationResources codeGeneration,
                DistinctKeySetPolicy distinctKeySetPolicy,
                AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy,
                FlatKeyTablePolicy flatKeyTablePolicy)
        {
            arrayPool = allocator.primitiveArrays();
            if (mask.none()) {
                return allocator.allocateSparseMask(allocationContext, EMPTY_POSITIONS, mask.size());
            }

            int inputOffset = grouped ? 1 : 0;
            if (grouped) {
                values[0] = groups;
            }
            if (distinctPositions.length < mask.selectedCount()) {
                int[] previous = distinctPositions;
                distinctPositions = arrayPool.borrowInts(mask.selectedCount());
                arrayPool.release(previous);
            }

            try {
                for (int index = 0; index < inputColumns.length; index++) {
                    values[index + inputOffset] = streamAccessor.values(inputColumns[index]);
                    nulls[index + inputOffset] = streamAccessor.nulls(inputColumns[index]);
                }
                if (distinctKeySet == null) {
                    distinctKeySet = groupPartitionedLongDistinct && inputColumns.length == 1
                            ? DistinctKeySet.createGroupedLong(
                                    values,
                                    inputTypes,
                                    allocator,
                                    allocationContext,
                                    arrayPool,
                                    codeGeneration,
                                    distinctKeySetPolicy,
                                    adaptiveLongGroupingPolicy,
                                    flatKeyTablePolicy,
                                    mask.selectedCount())
                            : DistinctKeySet.createWithUnboundPrefix(
                                    values,
                                    false,
                                    inputOffset,
                                    inputTypes,
                                    allocator,
                                    allocationContext,
                                    arrayPool,
                                    codeGeneration,
                                    distinctKeySetPolicy,
                                    adaptiveLongGroupingPolicy,
                                    flatKeyTablePolicy,
                                    mask.selectedCount());
                }
                int selectedCount = grouped
                        ? distinctKeySet.addGroupedBatch(values, nulls, mask, groupCount, distinctPositions)
                        : distinctKeySet.addBatch(values, nulls, mask, distinctPositions);
                return allocator.allocateSparseMask(allocationContext, distinctPositions, selectedCount, mask.size());
            }
            finally {
                Arrays.fill(values, null);
                Arrays.fill(nulls, null);
            }
        }

        void releaseBuffers()
        {
            if (distinctKeySet != null) {
                distinctKeySet.releaseBuffers();
                distinctKeySet = null;
            }
            if (arrayPool != null) {
                arrayPool.release(distinctPositions);
                distinctPositions = EMPTY_POSITIONS;
                arrayPool = null;
            }
        }
    }
}
