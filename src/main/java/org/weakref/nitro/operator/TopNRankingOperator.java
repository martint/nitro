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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.ArrayList;
import java.util.List;

public class TopNRankingOperator
        implements Operator
{
    private final Allocator.Context allocationContext = new Allocator.Context("TopNRankingOperator");

    private final Allocator allocator;
    private final Operator source;
    private final int[] orderingColumns;
    private final boolean[] descendingByColumn;
    private final int[] partitionColumns;
    private final int limit;

    private Streams[] materialized;
    private I64Vector ranks;
    private Mask outputMask;
    private boolean loaded;
    private boolean done;

    public TopNRankingOperator(Allocator allocator, int limit, int[] orderingColumns, boolean[] descendingByColumn, Operator source)
    {
        this(allocator, limit, new int[0], orderingColumns, descendingByColumn, source);
    }

    public TopNRankingOperator(Allocator allocator, int limit, int[] partitionColumns, int[] orderingColumns, boolean[] descendingByColumn, Operator source)
    {
        if (orderingColumns.length == 0) {
            throw new IllegalArgumentException("TopNRanking requires at least one ordering column");
        }
        if (orderingColumns.length != descendingByColumn.length) {
            throw new IllegalArgumentException("Ordering columns and directions must have the same length");
        }
        this.allocator = allocator;
        this.source = source;
        this.orderingColumns = orderingColumns.clone();
        this.descendingByColumn = descendingByColumn.clone();
        this.partitionColumns = partitionColumns.clone();
        this.limit = limit;
    }

    @Override
    public int outputCount()
    {
        return source.outputCount() + 1;
    }

    @Override
    public boolean hasNext()
    {
        if (!loaded) {
            load();
        }
        return !done;
    }

    @Override
    public Batch next()
    {
        if (!loaded) {
            load();
        }
        done = true;
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
            int index = outputIndex;
            outputs[outputIndex] = new Output(
                    materialized[index].streams(),
                    stream -> materialized[index].get(stream),
                    (stream, vector) -> allocator.transfer(allocationContext, vector));
        }
        outputs[source.outputCount()] = Output.of(Streams.ofValues(ranks));
        return new Batch(outputMask, takenMask -> allocator.transfer(allocationContext, takenMask), outputs);
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
    public void close()
    {
        source.close();
        allocator.release(allocationContext);
    }

    private void load()
    {
        loaded = true;

        List<TableOperator.Page> pages = new ArrayList<>();
        Streams[] schema = new Streams[source.outputCount()];
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                Mask mask = batch.borrowMask();
                for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
                    if (schema[outputIndex] == null) {
                        schema[outputIndex] = emptyStreamsLike(batch.output(outputIndex));
                    }
                }
                if (mask.none()) {
                    continue;
                }
                Streams[] columns = new Streams[source.outputCount()];
                for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
                    columns[outputIndex] = allocator.copyStreams(allocationContext, borrowedStreams(batch.output(outputIndex)), mask);
                }
                pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
            }
        }

        List<RowReference> rows = rows(pages);
        rows.sort(this::compareRows);

        List<RowReference> selected = new ArrayList<>();
        List<Long> selectedRanks = new ArrayList<>();
        RowReference previous = null;
        long partitionRowNumber = 0;
        long currentRank = 0;
        for (RowReference row : rows) {
            if (previous == null || !samePartition(previous, row)) {
                partitionRowNumber = 1;
                currentRank = 1;
            }
            else {
                partitionRowNumber++;
                if (!sameOrderingValue(previous, row)) {
                    currentRank = partitionRowNumber;
                }
            }
            if (currentRank <= limit) {
                selected.add(row);
                selectedRanks.add(currentRank);
            }
            previous = row;
        }

        materialized = new Streams[source.outputCount()];
        for (int outputIndex = 0; outputIndex < source.outputCount(); outputIndex++) {
            materialized[outputIndex] = materializeColumn(schema[outputIndex], outputIndex, selected, pages);
        }
        ranks = allocator.allocate(allocationContext, I64Vector.class, selectedRanks.size(), I64Vector::new);
        for (int index = 0; index < selectedRanks.size(); index++) {
            ranks.values()[index] = selectedRanks.get(index);
        }
        outputMask = allocator.allocateRangeMask(allocationContext, 0, selected.size());
    }

    private int compareRows(RowReference left, RowReference right)
    {
        for (int partitionColumn : partitionColumns) {
            int comparison = compareColumn(partitionColumn, left, right);
            if (comparison != 0) {
                return comparison;
            }
        }
        for (int orderingIndex = 0; orderingIndex < orderingColumns.length; orderingIndex++) {
            int comparison = compareColumn(orderingColumns[orderingIndex], left, right);
            if (descendingByColumn[orderingIndex]) {
                comparison = -comparison;
            }
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private boolean samePartition(RowReference left, RowReference right)
    {
        for (int partitionColumn : partitionColumns) {
            if (!equalColumn(partitionColumn, left, right)) {
                return false;
            }
        }
        return true;
    }

    private boolean sameOrderingValue(RowReference left, RowReference right)
    {
        for (int orderingColumn : orderingColumns) {
            if (!equalColumn(orderingColumn, left, right)) {
                return false;
            }
        }
        return true;
    }

    private int compareColumn(int column, RowReference left, RowReference right)
    {
        Streams leftStreams = left.page().columns()[column];
        Streams rightStreams = right.page().columns()[column];
        return OperatorOrderingSemantics.compare(
                leftStreams.values(),
                (BooleanVector) leftStreams.getOrNull(Stream.NULLS),
                left.position(),
                rightStreams.values(),
                (BooleanVector) rightStreams.getOrNull(Stream.NULLS),
                right.position());
    }

    private boolean equalColumn(int column, RowReference left, RowReference right)
    {
        Streams leftStreams = left.page().columns()[column];
        Streams rightStreams = right.page().columns()[column];
        return OperatorEqualitySemantics.equal(
                leftStreams.values(),
                (BooleanVector) leftStreams.getOrNull(Stream.NULLS),
                left.position(),
                rightStreams.values(),
                (BooleanVector) rightStreams.getOrNull(Stream.NULLS),
                right.position());
    }

    private Streams materializeColumn(Streams schema, int outputIndex, List<RowReference> rows, List<TableOperator.Page> pages)
    {
        Streams.Builder builder = Streams.builder();
        for (Stream stream : schema.streams()) {
            Vector result = null;
            for (int outputPosition = 0; outputPosition < rows.size(); outputPosition++) {
                RowReference row = rows.get(outputPosition);
                Streams sourceStreams = pages.get(row.pageIndex()).columns()[outputIndex];
                if (!sourceStreams.has(stream)) {
                    continue;
                }
                result = sourceStreams.get(stream).copySinglePositionInto(
                        allocator,
                        allocationContext,
                        result,
                        row.position(),
                        outputPosition,
                        rows.size());
            }
            builder.put(stream, result == null ? schema.get(stream).emptyLike(allocator, allocationContext) : result);
        }
        return builder.build();
    }

    private List<RowReference> rows(List<TableOperator.Page> pages)
    {
        List<RowReference> rows = new ArrayList<>();
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            TableOperator.Page page = pages.get(pageIndex);
            for (int position = 0; position < page.rows(); position++) {
                rows.add(new RowReference(pageIndex, page, position));
            }
        }
        return rows;
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

    private record RowReference(int pageIndex, TableOperator.Page page, int position) {}
}
