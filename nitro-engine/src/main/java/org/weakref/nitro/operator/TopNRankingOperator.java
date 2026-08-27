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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.data.VectorAccess;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TopNRankingOperator
        implements Operator
{
    public enum RankingType
    {
        ROW_NUMBER,
        RANK,
        DENSE_RANK
    }

    private final Allocator.Context allocationContext = new Allocator.Context("TopNRankingOperator");

    private final Allocator allocator;
    private final Operator source;
    private final int[] orderingColumns;
    private final boolean[] descendingByColumn;
    private final boolean[] nullsFirstByColumn;
    private final int[] partitionColumns;
    private final RankingType rankingType;
    private final int limit;
    private final int maxBatchRows;
    private final Schema outputSchema;
    private final StructuralComparisonKernel[] comparisonKernels;
    private final StructuralKeyKernel[] partitionKernels;

    private Streams[] sourceSchema;
    private List<TableOperator.Page> pages;
    private LongArrayList selectedRows;
    private I64Vector ranks;
    private int currentOutputPosition;
    private boolean loaded;

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            Operator source,
            TopNRankingOperatorPolicy policy)
    {
        this(
                allocator,
                limit,
                new int[0],
                orderingColumns,
                descendingByColumn,
                new boolean[orderingColumns.length],
                source,
                defaultRankingSchema(),
                policy,
                RankingType.RANK,
                EngineResources.from(allocator).operatorResources().codeGeneration().structuralTypes());
    }

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            Operator source,
            Schema rankingSchema,
            TopNRankingOperatorPolicy policy)
    {
        this(
                allocator,
                limit,
                new int[0],
                orderingColumns,
                descendingByColumn,
                new boolean[orderingColumns.length],
                source,
                rankingSchema,
                policy,
                RankingType.RANK,
                EngineResources.from(allocator).operatorResources().codeGeneration().structuralTypes());
    }

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            Operator source,
            TopNRankingOperatorPolicy policy)
    {
        this(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                new boolean[orderingColumns.length],
                source,
                defaultRankingSchema(),
                policy,
                RankingType.RANK,
                EngineResources.from(allocator).operatorResources().codeGeneration().structuralTypes());
    }

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            Operator source,
            Schema rankingSchema,
            TopNRankingOperatorPolicy policy)
    {
        this(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                new boolean[orderingColumns.length],
                source,
                rankingSchema,
                policy,
                RankingType.RANK,
                EngineResources.from(allocator).operatorResources().codeGeneration().structuralTypes());
    }

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            Operator source,
            Schema rankingSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                new boolean[orderingColumns.length],
                source,
                rankingSchema,
                requireNonNull(resources, "resources is null").topNRankingPolicy(),
                RankingType.RANK,
                resources.codeGeneration().structuralTypes());
    }

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            RankingType rankingType,
            Operator source,
            Schema rankingSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                new boolean[orderingColumns.length],
                source,
                rankingSchema,
                requireNonNull(resources, "resources is null").topNRankingPolicy(),
                rankingType,
                resources.codeGeneration().structuralTypes());
    }

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            boolean[] nullsFirstByColumn,
            RankingType rankingType,
            Operator source,
            Schema rankingSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descendingByColumn,
                nullsFirstByColumn,
                source,
                rankingSchema,
                requireNonNull(resources, "resources is null").topNRankingPolicy(),
                rankingType,
                resources.codeGeneration().structuralTypes());
    }

    private TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            boolean[] nullsFirstByColumn,
            Operator source,
            Schema rankingSchema,
            TopNRankingOperatorPolicy policy,
            RankingType rankingType,
            StructuralTypeKernelFactory structuralTypes)
    {
        if (limit <= 0) {
            throw new IllegalArgumentException("TopNRanking limit must be positive");
        }
        if (orderingColumns.length == 0) {
            throw new IllegalArgumentException("TopNRanking requires at least one ordering column");
        }
        if (orderingColumns.length != descendingByColumn.length || orderingColumns.length != nullsFirstByColumn.length) {
            throw new IllegalArgumentException("Ordering columns, directions, and null placements must have the same length");
        }
        rankingSchema = requireNonNull(rankingSchema, "rankingSchema is null");
        if (rankingSchema.size() != 1) {
            throw new IllegalArgumentException("rankingSchema must contain exactly one field");
        }
        this.allocator = allocator;
        this.source = source;
        this.orderingColumns = orderingColumns.clone();
        this.descendingByColumn = descendingByColumn.clone();
        this.nullsFirstByColumn = nullsFirstByColumn.clone();
        this.partitionColumns = partitionColumns.clone();
        this.rankingType = requireNonNull(rankingType, "rankingType is null");
        this.limit = limit;
        this.maxBatchRows = requireNonNull(policy, "policy is null").maxBatchRows();
        this.outputSchema = outputSchema(source.outputSchema(), rankingSchema);
        this.comparisonKernels = comparisonKernels(
                source.outputSchema(),
                this.partitionColumns,
                this.orderingColumns,
                this.nullsFirstByColumn,
                requireNonNull(structuralTypes, "structuralTypes is null"));
        this.partitionKernels = partitionKernels(
                source.outputSchema(),
                this.partitionColumns,
                structuralTypes);
    }

    @Override
    public int outputCount()
    {
        return source.outputCount() + 1;
    }

    @Override
    public Schema outputSchema()
    {
        return outputSchema;
    }

    private static Schema defaultRankingSchema()
    {
        Field unspecified = Schema.unspecified(1).field(0);
        return new Schema(List.of(new Field(unspecified.type(), false)));
    }

    private static Schema outputSchema(Schema sourceSchema, Schema rankingSchema)
    {
        List<Field> fields = new ArrayList<>(sourceSchema.fields());
        fields.add(rankingSchema.field(0));
        return new Schema(fields);
    }

    private static StructuralComparisonKernel[] comparisonKernels(
            Schema sourceSchema,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] nullsFirstByColumn,
            StructuralTypeKernelFactory structuralTypes)
    {
        StructuralComparisonKernel[] kernels = new StructuralComparisonKernel[sourceSchema.size()];
        for (int column : partitionColumns) {
            kernels[column] = structuralTypes.comparison(sourceSchema.field(column).type());
        }
        for (int index = 0; index < orderingColumns.length; index++) {
            int column = orderingColumns[index];
            kernels[column] = structuralTypes.comparison(
                    sourceSchema.field(column).type(), nullsFirstByColumn[index]);
        }
        return kernels;
    }

    private static StructuralKeyKernel[] partitionKernels(
            Schema sourceSchema,
            int[] partitionColumns,
            StructuralTypeKernelFactory structuralTypes)
    {
        StructuralKeyKernel[] kernels = new StructuralKeyKernel[sourceSchema.size()];
        for (int column : partitionColumns) {
            kernels[column] = structuralTypes.key(sourceSchema.field(column).type());
        }
        return kernels;
    }

    @Override
    public boolean hasNext()
    {
        if (!loaded) {
            load();
        }
        return currentOutputPosition < selectedRows.size();
    }

    @Override
    public Batch next()
    {
        if (!loaded) {
            load();
        }
        int batchSize = Math.min(maxBatchRows, selectedRows.size() - currentOutputPosition);
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
            Streams batchStreams = materializeSourceColumnBatch(outputIndex, currentOutputPosition, batchSize);
            outputs[outputIndex] = new Output(
                    batchStreams.streams(),
                    batchStreams::get,
                    (stream, vector) -> allocator.transfer(allocationContext, vector),
                    (stream, vector) -> allocator.release(allocationContext, vector));
        }
        Streams ranksBatch = materializeRanksBatch(currentOutputPosition, batchSize);
        outputs[source.outputCount()] = new Output(
                ranksBatch.streams(),
                ranksBatch::get,
                (stream, vector) -> allocator.transfer(allocationContext, vector),
                (stream, vector) -> allocator.release(allocationContext, vector));
        currentOutputPosition += batchSize;
        Mask outputMask = allocator.allocateRangeMask(allocationContext, 0, batchSize);
        return new Batch(
                outputMask,
                _ -> {},
                takenMask -> allocator.transfer(allocationContext, takenMask),
                batchMask -> allocator.release(allocationContext, batchMask),
                () -> {},
                outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // Output is fully computed and constrain() is a no-op, so a downstream constrain + re-borrow
        // would re-read the full, differently-indexed output. Cannot satisfy a constrained re-borrow.
        return false;
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(allocationContext);
    }

    private void load()
    {
        loaded = true;

        pages = new ArrayList<>();
        sourceSchema = new Streams[source.outputCount()];
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
                    if (sourceSchema[outputIndex] == null) {
                        sourceSchema[outputIndex] = emptyStreamsLike(batch.output(outputIndex));
                    }
                }
                if (mask.none()) {
                    continue;
                }
                if (source.supportsRetainedBatches()) {
                    Streams[] retainedColumns = new Streams[source.outputCount()];
                    for (int outputIndex = 0; outputIndex < retainedColumns.length; outputIndex++) {
                        retainedColumns[outputIndex] = takeStreams(batch.output(outputIndex));
                    }
                    pages.add(new TableOperator.Page(mask.count(), retainedColumns, allocator.transfer(allocationContext, batch.takeMask())));
                    continue;
                }
                Streams[] columns = new Streams[source.outputCount()];
                for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                    columns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(batch.output(outputIndex)), mask);
                }
                pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
            }
        }

        // A full global sort would order every row by (partition, ordering) only to keep the rank<=limit
        // prefix of each partition. Instead, bucket rows by partition and select a bounded top-N per
        // bucket: the comparator never compares partition columns, and per-partition work scales with
        // the limit rather than the partition size.
        List<RankedRow> ranked = new ArrayList<>();
        if (limit > 0) {
            Map<PartitionKey, LongArrayList> partitions = partitions();
            for (LongArrayList partition : partitions.values()) {
                rankPartition(partition, ranked);
            }
        }

        // Restore the global (partition, ordering) order a full sort would have produced so output order
        // and rank alignment are identical regardless of how rows were bucketed. The ranked set is
        // bounded (~limit per partition), so this sort is cheap.
        ranked.sort((left, right) -> compareRows(left.row(), right.row()));

        LongArrayList selected = new LongArrayList(ranked.size());
        List<Long> selectedRanks = new ArrayList<>(ranked.size());
        for (RankedRow row : ranked) {
            selected.add(row.row());
            selectedRanks.add(row.rank());
        }

        selectedRows = selected;
        ranks = allocator.allocate(allocationContext, I64Vector.class, selectedRanks.size(), I64Vector::new);
        for (int index = 0; index < selectedRanks.size(); index++) {
            ranks.values()[index] = selectedRanks.get(index);
        }
    }

    private Map<PartitionKey, LongArrayList> partitions()
    {
        Map<PartitionKey, LongArrayList> partitions = new HashMap<>();
        PartitionKey probe = new PartitionKey();
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            if (page.mask().all()) {
                for (int position = 0; position < page.rows(); position++) {
                    addPartitionRow(partitions, probe, rowReference(pageIndex, position));
                }
                continue;
            }
            for (int index = 0; index < page.mask().selectedCount(); index++) {
                addPartitionRow(partitions, probe, rowReference(pageIndex, page.mask().position(index)));
            }
        }
        return partitions;
    }

    private void addPartitionRow(Map<PartitionKey, LongArrayList> partitions, PartitionKey probe, long row)
    {
        probe.set(row);
        LongArrayList partition = partitions.get(probe);
        if (partition == null) {
            partition = new LongArrayList();
            partitions.put(new PartitionKey(row), partition);
        }
        partition.add(row);
    }

    private int compareRows(long left, long right)
    {
        for (int partitionColumn : partitionColumns) {
            int comparison = compareColumn(partitionColumn, left, right);
            if (comparison != 0) {
                return comparison;
            }
        }
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int comparison = compareOrderingColumn(orderingIndex, left, right);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private void rankPartition(LongArrayList partition, List<RankedRow> ranked)
    {
        LongArrayList candidates = rankingType == RankingType.DENSE_RANK
                ? new LongArrayList(partition)
                : boundedTopN(partition);
        candidates.sort(this::compareOrdering);

        long previous = 0;
        boolean first = true;
        long partitionRowNumber = 0;
        long currentRank = 0;
        long currentDenseRank = 0;
        for (int index = 0; index < candidates.size(); index++) {
            long row = candidates.getLong(index);
            if (first) {
                partitionRowNumber = 1;
                currentRank = 1;
                currentDenseRank = 1;
                first = false;
            }
            else {
                partitionRowNumber++;
                if (!sameOrderingValue(previous, row)) {
                    currentRank = partitionRowNumber;
                    currentDenseRank++;
                }
            }
            long outputRank = switch (rankingType) {
                case ROW_NUMBER -> partitionRowNumber;
                case RANK -> currentRank;
                case DENSE_RANK -> currentDenseRank;
            };
            if (outputRank > limit) {
                if (rankingType == RankingType.DENSE_RANK) {
                    break;
                }
                previous = row;
                continue;
            }
            ranked.add(new RankedRow(row, outputRank));
            previous = row;
        }
    }

    /**
     * Returns the smallest set of rows that contains every row of the partition with {@code rank <= limit}.
     * The limit-th best row (the rank threshold) is found with a bounded min-heap; every row ordering
     * better than or equal to it is a candidate. For partitions no larger than the limit, every row
     * qualifies.
     */
    private LongArrayList boundedTopN(LongArrayList partition)
    {
        if (partition.size() <= limit) {
            return partition;
        }
        // Min-heap whose root is the worst of the best `limit` rows seen so far.
        PriorityQueue<Long> bestRows = new PriorityQueue<>(limit, (left, right) -> compareOrdering(right, left));
        for (int index = 0; index < partition.size(); index++) {
            long row = partition.getLong(index);
            if (bestRows.size() < limit) {
                bestRows.add(row);
            }
            else if (compareOrdering(row, bestRows.peek()) < 0) {
                bestRows.poll();
                bestRows.add(row);
            }
        }
        long threshold = bestRows.peek();
        LongArrayList candidates = new LongArrayList();
        for (int index = 0; index < partition.size(); index++) {
            long row = partition.getLong(index);
            if (compareOrdering(row, threshold) <= 0) {
                candidates.add(row);
            }
        }
        return candidates;
    }

    private int compareOrdering(long left, long right)
    {
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int comparison = compareOrderingColumn(orderingIndex, left, right);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private boolean sameOrderingValue(long left, long right)
    {
        for (int orderingColumn : orderingColumns) {
            if (!equalColumn(orderingColumn, left, right)) {
                return false;
            }
        }
        return true;
    }

    private int compareColumn(int column, long left, long right)
    {
        Streams leftStreams = pages.get(pageIndex(left)).columns()[column];
        Streams rightStreams = pages.get(pageIndex(right)).columns()[column];
        return comparisonKernels[column].compare(
                leftStreams.values(),
                leftStreams.getOrNull(Stream.NULLS),
                pagePosition(left),
                rightStreams.values(),
                rightStreams.getOrNull(Stream.NULLS),
                pagePosition(right));
    }

    private boolean equalColumn(int column, long left, long right)
    {
        Streams leftStreams = pages.get(pageIndex(left)).columns()[column];
        Streams rightStreams = pages.get(pageIndex(right)).columns()[column];
        boolean leftNull = OperatorVectorSupport.isNull(leftStreams.getOrNull(Stream.NULLS), pagePosition(left));
        boolean rightNull = OperatorVectorSupport.isNull(rightStreams.getOrNull(Stream.NULLS), pagePosition(right));
        if (leftNull || rightNull) {
            return leftNull == rightNull;
        }
        return comparisonKernels[column].identical(
                leftStreams.values(),
                leftStreams.getOrNull(Stream.NULLS),
                pagePosition(left),
                rightStreams.values(),
                rightStreams.getOrNull(Stream.NULLS),
                pagePosition(right));
    }

    private int compareOrderingColumn(int orderingIndex, long left, long right)
    {
        int column = orderingColumns[orderingIndex];
        Streams leftStreams = pages.get(pageIndex(left)).columns()[column];
        Streams rightStreams = pages.get(pageIndex(right)).columns()[column];
        boolean leftNull = OperatorVectorSupport.isNull(leftStreams.getOrNull(Stream.NULLS), pagePosition(left));
        boolean rightNull = OperatorVectorSupport.isNull(rightStreams.getOrNull(Stream.NULLS), pagePosition(right));
        if (leftNull || rightNull) {
            if (leftNull == rightNull) {
                return 0;
            }
            return leftNull == nullsFirstByColumn[orderingIndex] ? -1 : 1;
        }
        int comparison = comparisonKernels[column].compare(
                leftStreams.values(),
                leftStreams.getOrNull(Stream.NULLS),
                pagePosition(left),
                rightStreams.values(),
                rightStreams.getOrNull(Stream.NULLS),
                pagePosition(right));
        return descendingByColumn[orderingIndex] ? -comparison : comparison;
    }

    private Streams materializeSourceColumnBatch(int outputIndex, int startPosition, int batchSize)
    {
        Streams.Builder builder = Streams.builder();
        Streams schema = sourceSchema[outputIndex];
        for (Stream stream : schema.streams()) {
            Vector result = null;
            int outputPosition = 0;
            while (outputPosition < batchSize) {
                long firstRow = selectedRows.getLong(startPosition + outputPosition);
                Streams sourceStreams = pages.get(pageIndex(firstRow)).columns()[outputIndex];
                if (!sourceStreams.has(stream)) {
                    outputPosition++;
                    continue;
                }

                int groupStart = outputPosition;
                int groupPageIndex = pageIndex(firstRow);
                while (outputPosition < batchSize && pageIndex(selectedRows.getLong(startPosition + outputPosition)) == groupPageIndex) {
                    outputPosition++;
                }

                int groupSize = outputPosition - groupStart;
                int[] positions = new int[groupSize];
                for (int index = 0; index < groupSize; index++) {
                    positions[index] = pagePosition(selectedRows.getLong(startPosition + groupStart + index));
                }
                result = copyPositionsInto(
                        schema.get(stream),
                        sourceStreams.get(stream),
                        stream == Stream.VALUES ? sourceStreams.getOrNull(Stream.NULLS) : null,
                        result,
                        positions,
                        groupSize,
                        groupStart,
                        batchSize);
            }
            builder.put(stream, result == null ? schema.get(stream).emptyLike(allocator, allocationContext) : result);
        }
        return builder.build();
    }

    private Vector copyPositionsInto(
            Vector schema,
            Vector source,
            Vector sourceNulls,
            Vector existing,
            int[] sourcePositions,
            int sourceCount,
            int outputStart,
            int size)
    {
        if (source instanceof BinaryVector binary) {
            int currentOffset = outputStart == 0 ? 0 : ((BinaryVector) existing).endOffset(outputStart - 1);
            int byteCapacity = currentOffset;
            for (int index = 0; index < sourceCount; index++) {
                int sourcePosition = sourcePositions[index];
                if (!OperatorVectorSupport.isNull(sourceNulls, sourcePosition)) {
                    byteCapacity = Math.addExact(byteCapacity, binary.length(sourcePosition));
                }
            }
            BinaryVector target = BinaryVector.allocateOrGrow(
                    allocator,
                    allocationContext,
                    (BinaryVector) existing,
                    size,
                    byteCapacity,
                    currentOffset);
            if (outputStart == 0) {
                Arrays.fill(target.offsets(), 0);
                target.clearTraits();
                target.addTraits(binary.traits());
            }
            for (int index = 0; index < sourceCount; index++) {
                int targetPosition = outputStart + index;
                target.offsets()[targetPosition] = currentOffset;
                int sourcePosition = sourcePositions[index];
                if (OperatorVectorSupport.isNull(sourceNulls, sourcePosition)) {
                    target.setNull(targetPosition);
                }
                else {
                    target.setBytes(targetPosition, binary.data(), binary.startOffset(sourcePosition), binary.length(sourcePosition));
                    currentOffset = target.endOffset(targetPosition);
                }
            }
            return target;
        }
        if (schema instanceof I64Vector) {
            I64Vector target = allocator.allocateOrGrow(allocationContext, (I64Vector) existing, I64Vector.class, size, I64Vector::new);
            VectorAccess.LongValues values = VectorAccess.longValues(source);
            for (int index = 0; index < sourceCount; index++) {
                target.values()[outputStart + index] = values.value(sourcePositions[index]);
            }
            return target;
        }
        if (schema instanceof I32Vector) {
            I32Vector target = allocator.allocateOrGrow(allocationContext, (I32Vector) existing, I32Vector.class, size, I32Vector::new);
            VectorAccess.LongValues values = VectorAccess.longValues(source);
            for (int index = 0; index < sourceCount; index++) {
                target.values()[outputStart + index] = toIntExact(values.value(sourcePositions[index]));
            }
            return target;
        }
        return source.copyPositionsInto(
                allocator,
                allocationContext,
                existing,
                sourcePositions,
                sourceCount,
                outputStart,
                size);
    }

    private Streams materializeRanksBatch(int startPosition, int batchSize)
    {
        I64Vector batchRanks = allocator.allocate(allocationContext, I64Vector.class, batchSize, I64Vector::new);
        System.arraycopy(ranks.values(), startPosition, batchRanks.values(), 0, batchSize);
        return Streams.ofValues(batchRanks);
    }

    private Streams emptyStreamsLike(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream).emptyLike(allocator, allocationContext));
        }
        return builder.build();
    }

    private static Streams borrowedStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, output.borrow(stream));
        }
        return builder.build();
    }

    private Streams takeStreams(Output output)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : output.streams()) {
            builder.put(stream, allocator.transfer(allocationContext, output.take(stream)));
        }
        return builder.build();
    }

    private static long rowReference(int pageIndex, int position)
    {
        return ((long) pageIndex << Integer.SIZE) | Integer.toUnsignedLong(position);
    }

    private static int pageIndex(long rowReference)
    {
        return (int) (rowReference >>> Integer.SIZE);
    }

    private static int pagePosition(long rowReference)
    {
        return (int) rowReference;
    }

    private record RankedRow(long row, long rank) {}

    /**
     * Groups rows by partition value using NOT DISTINCT equality, including nulls.
     */
    private final class PartitionKey
    {
        private long row;
        private int hash;

        private PartitionKey() {}

        private PartitionKey(long row)
        {
            set(row);
        }

        private void set(long row)
        {
            this.row = row;
            int result = 1;
            for (int partitionColumn : partitionColumns) {
                Streams streams = pages.get(pageIndex(row)).columns()[partitionColumn];
                result = 31 * result + Long.hashCode(partitionKernels[partitionColumn].hash(
                        streams.values(),
                        streams.getOrNull(Stream.NULLS),
                        pagePosition(row)));
            }
            this.hash = result;
        }

        @Override
        public int hashCode()
        {
            return hash;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PartitionKey that)) {
                return false;
            }
            if (hash != that.hash) {
                return false;
            }
            for (int partitionColumn : partitionColumns) {
                Streams left = pages.get(pageIndex(row)).columns()[partitionColumn];
                Streams right = pages.get(pageIndex(that.row)).columns()[partitionColumn];
                boolean leftNull = OperatorVectorSupport.isNull(left.getOrNull(Stream.NULLS), pagePosition(row));
                boolean rightNull = OperatorVectorSupport.isNull(right.getOrNull(Stream.NULLS), pagePosition(that.row));
                if (leftNull || rightNull) {
                    if (leftNull != rightNull) {
                        return false;
                    }
                    continue;
                }
                if (!partitionKernels[partitionColumn].identical(
                        left.values(),
                        left.getOrNull(Stream.NULLS),
                        pagePosition(row),
                        right.values(),
                        right.getOrNull(Stream.NULLS),
                        pagePosition(that.row))) {
                    return false;
                }
            }
            return true;
        }
    }
}
