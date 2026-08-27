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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Host-driven partitioned TopN-ranking state. Offered batches remain caller-owned and are copied
 * before {@link #addInput(Batch)} returns.
 */
public final class TopNRankingSession
        implements Operator
{
    private final Allocator.Context allocationContext =
            new Allocator.Context("TopNRankingSession", TopNRankingSession.class);
    private final Allocator allocator;
    private final int inputColumns;
    private final List<TableOperator.Page> pages = new ArrayList<>();
    private final TopNRankingOperator ranking;
    private boolean finished;
    private boolean closed;

    public TopNRankingSession(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descending,
            TopNRankingOperator.RankingType rankingType,
            Schema inputSchema,
            Schema rankingSchema,
            OperatorResources resources)
    {
        this(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descending,
                new boolean[orderingColumns.length],
                rankingType,
                inputSchema,
                rankingSchema,
                resources);
    }

    public TopNRankingSession(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            TopNRankingOperator.RankingType rankingType,
            Schema inputSchema,
            Schema rankingSchema,
            OperatorResources resources)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        inputColumns = requireNonNull(inputSchema, "inputSchema is null").size();
        ranking = new TopNRankingOperator(
                allocator,
                limit,
                partitionColumns,
                orderingColumns,
                descending,
                nullsFirst,
                rankingType,
                TableOperator.retained(inputSchema, pages),
                rankingSchema,
                requireNonNull(resources, "resources is null"));
    }

    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is finished");
        }
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            return;
        }
        Streams[] columns = new Streams[inputColumns];
        for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
            Output output = batch.output(outputIndex);
            Streams.Builder borrowed = Streams.builder();
            for (Stream stream : output.streams()) {
                borrowed.put(stream, output.borrow(stream));
            }
            columns[outputIndex] = allocator.copyStreams(allocationContext, borrowed.build(), mask);
        }
        pages.add(new TableOperator.Page(mask.count(), columns, Mask.all(mask.count())));
    }

    /**
     * Adds an allocator-owned native batch without copying its streams. Ownership of every stream and the
     * selection transfers to this session; the caller must still close the now-drained batch.
     */
    public void addRetainedInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is finished");
        }
        Mask mask = batch.borrowMask();
        if (mask.none()) {
            return;
        }
        Streams[] columns = new Streams[inputColumns];
        for (int outputIndex = 0; outputIndex < columns.length; outputIndex++) {
            Output output = batch.output(outputIndex);
            Streams.Builder retained = Streams.builder();
            for (Stream stream : output.streams()) {
                retained.put(stream, allocator.transfer(allocationContext, output.take(stream)));
            }
            columns[outputIndex] = retained.build();
        }
        pages.add(new TableOperator.Page(
                mask.count(),
                columns,
                allocator.transfer(allocationContext, batch.takeMask())));
    }

    public void finishInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is already finished");
        }
        finished = true;
    }

    @Override
    public int outputCount()
    {
        return ranking.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return ranking.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        checkFinished();
        return ranking.hasNext();
    }

    @Override
    public Batch next()
    {
        checkFinished();
        return ranking.next();
    }

    @Override
    public void constrain(Mask mask)
    {
        ranking.constrain(mask);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            ranking.close();
        }
        finally {
            allocator.release(allocationContext);
        }
    }

    private void checkFinished()
    {
        checkOpen();
        if (!finished) {
            throw new IllegalStateException("TopN ranking input is not finished");
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("TopN ranking session is closed");
        }
    }
}
