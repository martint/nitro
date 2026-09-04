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

import org.weakref.nitro.core.type.Field;
import org.weakref.nitro.core.type.Schema;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.execution.EngineResources;

import java.util.ArrayList;
import java.util.List;

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

    private final Operator source;
    private final Schema outputSchema;
    private final TopNRankingState rankingState;
    private boolean loaded;

    public TopNRankingOperator(Allocator allocator, int limit, int[] orderingColumns, boolean[] descendingByColumn, Operator source, TopNRankingOperatorPolicy policy)
    {
        this(allocator, limit, new int[0], orderingColumns, descendingByColumn, new boolean[orderingColumns.length], source,
                defaultRankingSchema(), policy, RankingType.RANK, true, EngineResources.from(allocator).operatorResources());
    }

    public TopNRankingOperator(Allocator allocator, int limit, int[] orderingColumns, boolean[] descendingByColumn, Operator source, Schema rankingSchema, TopNRankingOperatorPolicy policy)
    {
        this(allocator, limit, new int[0], orderingColumns, descendingByColumn, new boolean[orderingColumns.length], source,
                rankingSchema, policy, RankingType.RANK, true, EngineResources.from(allocator).operatorResources());
    }

    public TopNRankingOperator(Allocator allocator, int limit, int[] partitionColumns, int[] orderingColumns, boolean[] descendingByColumn, Operator source, TopNRankingOperatorPolicy policy)
    {
        this(allocator, limit, partitionColumns, orderingColumns, descendingByColumn, new boolean[orderingColumns.length], source,
                defaultRankingSchema(), policy, RankingType.RANK, true, EngineResources.from(allocator).operatorResources());
    }

    public TopNRankingOperator(Allocator allocator, int limit, int[] partitionColumns, int[] orderingColumns, boolean[] descendingByColumn, Operator source, Schema rankingSchema, TopNRankingOperatorPolicy policy)
    {
        this(allocator, limit, partitionColumns, orderingColumns, descendingByColumn, new boolean[orderingColumns.length], source,
                rankingSchema, policy, RankingType.RANK, true, EngineResources.from(allocator).operatorResources());
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
        this(allocator, limit, partitionColumns, orderingColumns, descendingByColumn, new boolean[orderingColumns.length], source,
                rankingSchema, requireNonNull(resources, "resources is null").topNRankingPolicy(), RankingType.RANK, true, resources);
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
        this(allocator, limit, partitionColumns, orderingColumns, descendingByColumn, new boolean[orderingColumns.length], source,
                rankingSchema, requireNonNull(resources, "resources is null").topNRankingPolicy(), rankingType, true, resources);
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
        this(allocator, limit, partitionColumns, orderingColumns, descendingByColumn, nullsFirstByColumn, rankingType,
                true, source, rankingSchema, resources);
    }

    public TopNRankingOperator(
            Allocator allocator,
            int limit,
            int[] partitionColumns,
            int[] orderingColumns,
            boolean[] descendingByColumn,
            boolean[] nullsFirstByColumn,
            RankingType rankingType,
            boolean outputRanking,
            Operator source,
            Schema rankingSchema,
            OperatorResources resources)
    {
        this(allocator, limit, partitionColumns, orderingColumns, descendingByColumn, nullsFirstByColumn, source,
                rankingSchema, requireNonNull(resources, "resources is null").topNRankingPolicy(), rankingType, outputRanking, resources);
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
            boolean outputRanking,
            OperatorResources resources)
    {
        requireNonNull(allocator, "allocator is null");
        requireNonNull(partitionColumns, "partitionColumns is null");
        requireNonNull(orderingColumns, "orderingColumns is null");
        requireNonNull(descendingByColumn, "descendingByColumn is null");
        requireNonNull(nullsFirstByColumn, "nullsFirstByColumn is null");
        requireNonNull(policy, "policy is null");
        resources = requireNonNull(resources, "resources is null");
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
        this.source = requireNonNull(source, "source is null");
        this.outputSchema = outputSchema(source.outputSchema(), rankingSchema, outputRanking);
        rankingType = requireNonNull(rankingType, "rankingType is null");
        this.rankingState = partitionColumns.length == 0
                ? new UnpartitionedTopNRankingState(
                        allocator, limit, orderingColumns, descendingByColumn, nullsFirstByColumn, rankingType,
                        outputRanking, source.outputSchema(), rankingSchema, resources.joinBufferPolicy(),
                        resources.codeGeneration().structuralTypes(), policy)
                : new PartitionedTopNRankingState(
                        allocator, limit, partitionColumns, orderingColumns, descendingByColumn, nullsFirstByColumn,
                        rankingType, outputRanking, source.outputSchema(), rankingSchema, resources, policy);
    }

    @Override
    public int outputCount()
    {
        return rankingState.outputCount();
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

    private static Schema outputSchema(Schema sourceSchema, Schema rankingSchema, boolean outputRanking)
    {
        if (!outputRanking) {
            return sourceSchema;
        }
        List<Field> fields = new ArrayList<>(sourceSchema.fields());
        fields.add(rankingSchema.field(0));
        return new Schema(fields);
    }

    @Override
    public boolean hasNext()
    {
        load();
        return rankingState.hasNext();
    }

    @Override
    public Batch next()
    {
        load();
        return rankingState.next();
    }

    @Override
    public void constrain(Mask mask)
    {
        rankingState.constrain(mask);
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return true;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        return false;
    }

    @Override
    public void close()
    {
        try {
            rankingState.close();
        }
        finally {
            source.close();
        }
    }

    private void load()
    {
        if (loaded) {
            return;
        }
        while (source.hasNext()) {
            try (Batch batch = source.next()) {
                rankingState.addInput(batch);
            }
        }
        rankingState.finishInput();
        loaded = true;
    }
}
