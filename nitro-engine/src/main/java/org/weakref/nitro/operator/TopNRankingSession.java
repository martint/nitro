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

import static java.util.Objects.requireNonNull;

/**
 * Host-driven TopN-ranking state. Offered batches remain caller-owned and are copied
 * before {@link #addInput(Batch)} returns.
 */
public final class TopNRankingSession
        implements Operator
{
    private final TopNRankingState ranking;
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
        requireNonNull(allocator, "allocator is null");
        requireNonNull(inputSchema, "inputSchema is null");
        resources = requireNonNull(resources, "resources is null");
        if (partitionColumns.length == 0) {
            ranking = new UnpartitionedTopNRankingState(
                    allocator,
                    limit,
                    orderingColumns,
                    descending,
                    nullsFirst,
                    rankingType,
                    true,
                    inputSchema,
                    rankingSchema,
                    resources);
        }
        else {
            ranking = new PartitionedTopNRankingState(
                    allocator,
                    limit,
                    partitionColumns,
                    orderingColumns,
                    descending,
                    nullsFirst,
                    rankingType,
                    true,
                    inputSchema,
                    rankingSchema,
                    resources);
        }
    }

    public void addInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is finished");
        }
        ranking.addInput(batch);
    }

    /**
     * Adds an allocator-owned native batch. Bounded state copies only qualifying rows. The caller relinquishes the
     * contents and must close the batch after this method returns, whether or not it was physically drained.
     */
    public void addRetainedInput(Batch batch)
    {
        requireNonNull(batch, "batch is null");
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is finished");
        }
        ranking.addInput(batch);
    }

    public void finishInput()
    {
        checkOpen();
        if (finished) {
            throw new IllegalStateException("TopN ranking input is already finished");
        }
        finished = true;
        ranking.finishInput();
    }

    @Override
    public int outputCount()
    {
        return delegate().outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return delegate().outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        checkFinished();
        return delegate().hasNext();
    }

    @Override
    public Batch next()
    {
        checkFinished();
        return delegate().next();
    }

    @Override
    public void constrain(Mask mask)
    {
        delegate().constrain(mask);
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
        ranking.close();
    }

    private void checkFinished()
    {
        checkOpen();
        if (!finished) {
            throw new IllegalStateException("TopN ranking input is not finished");
        }
    }

    private Operator delegate()
    {
        return ranking;
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("TopN ranking session is closed");
        }
    }
}
