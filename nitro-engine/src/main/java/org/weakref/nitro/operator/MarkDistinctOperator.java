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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.PrimitiveArrayPool;
import org.weakref.nitro.data.Stream;
import org.weakref.nitro.data.Vector;

import java.util.Arrays;
import java.util.List;

public class MarkDistinctOperator
        implements Operator
{
    private static final int[] EMPTY_POSITIONS = new int[0];

    private final Allocator allocator;
    private final Allocator.Context allocationContext;
    private final PrimitiveArrayPool arrayPool;
    private final Operator source;
    private final OperatorCodeGenerationResources codeGeneration;
    private final DistinctKeySetPolicy distinctKeySetPolicy;
    private final AdaptiveLongGroupingPolicy adaptiveLongGroupingPolicy;
    private final FlatKeyTablePolicy flatKeyTablePolicy;
    private final int[] distinctColumns;
    private final List<TypeBinding> distinctTypes;
    private final boolean retainNulls;
    private final Vector[] values;
    private final Vector[] nulls;

    private DistinctKeySet distinctKeySet;
    private int[] distinctPositions = EMPTY_POSITIONS;
    private BatchState currentBatchState;

    public MarkDistinctOperator(Allocator allocator, int distinctColumn, Operator source, OperatorResources operatorResources)
    {
        this(allocator, new int[] {distinctColumn}, source, false, operatorResources);
    }

    public MarkDistinctOperator(Allocator allocator, int[] distinctColumns, Operator source, OperatorResources operatorResources)
    {
        this(allocator, distinctColumns, source, false, operatorResources);
    }

    /**
     * Creates a mark-distinct operator over the given key columns.
     *
     * @param retainNulls when {@code true}, null-keyed rows are kept and de-duplicated with SQL
     * {@code DISTINCT}/{@code UNION} semantics (equal nulls collapse, nulls stay distinct from concrete values);
     * when {@code false}, any row with a NULL key column is dropped, matching {@code count(distinct ...)}.
     */
    public MarkDistinctOperator(Allocator allocator, int[] distinctColumns, Operator source, boolean retainNulls, OperatorResources operatorResources)
    {
        this.allocator = allocator;
        this.allocationContext = new Allocator.Context(
                "MarkDistinctOperator",
                operatorResources.grouping().markDistinctMaskPool());
        this.arrayPool = allocator.primitiveArrays();
        this.codeGeneration = operatorResources.codeGeneration();
        this.distinctKeySetPolicy = operatorResources.distinctKeySetPolicy();
        this.adaptiveLongGroupingPolicy = operatorResources.adaptiveLongGroupingPolicy();
        this.flatKeyTablePolicy = operatorResources.flatKeyTablePolicy();
        this.source = source;
        this.distinctColumns = distinctColumns.clone();
        this.distinctTypes = distinctTypes(source.outputSchema(), distinctColumns);
        this.retainNulls = retainNulls;
        this.values = new Vector[distinctColumns.length];
        this.nulls = new Vector[distinctColumns.length];
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
    }

    @Override
    public Schema outputSchema()
    {
        return source.outputSchema();
    }

    @Override
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        Mask sourceMask = sourceBatch.borrowMask();
        Mask batchMask = computeDistinctMask(sourceBatch, sourceMask);
        if (batchMask == sourceMask) {
            currentBatchState = null;
            return sourceBatch;
        }
        source.constrain(batchMask);
        sourceBatch.constrain(batchMask);

        BatchState batchState = new BatchState(sourceBatch, batchMask);
        currentBatchState = batchState;

        return Batch.forwarding(batchMask, batchState, sourceBatch);
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return source.supportsRetainedBatches();
    }

    @Override
    public boolean supportsOpenBatchHasNext()
    {
        return source.supportsOpenBatchHasNext();
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // The output adds a distinct-marker column relative to the source, so it cannot be
        // re-borrowed by source position after a downstream constrain.
        return false;
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch().close();
            currentBatchState = null;
        }
        source.close();
        if (distinctKeySet != null) {
            distinctKeySet.releaseBuffers();
            distinctKeySet = null;
        }
        arrayPool.release(distinctPositions);
        distinctPositions = EMPTY_POSITIONS;
        Arrays.fill(values, null);
        Arrays.fill(nulls, null);
        allocator.release(allocationContext);
    }

    private Mask computeDistinctMask(Batch sourceBatch, Mask sourceMask)
    {
        if (sourceMask.none()) {
            return sourceMask;
        }

        if (distinctPositions.length < sourceMask.selectedCount()) {
            int[] previous = distinctPositions;
            distinctPositions = arrayPool.borrowInts(sourceMask.selectedCount());
            arrayPool.release(previous);
        }

        int selectedCount;
        try {
            for (int index = 0; index < distinctColumns.length; index++) {
                Output output = sourceBatch.output(distinctColumns[index]);
                values[index] = output.borrow(Stream.VALUES);
                nulls[index] = output.borrowOrNull(Stream.NULLS);
            }
            if (distinctKeySet == null) {
                distinctKeySet = DistinctKeySet.create(
                        values,
                        retainNulls,
                        distinctTypes,
                        allocator,
                        allocationContext,
                        arrayPool,
                        codeGeneration,
                        distinctKeySetPolicy,
                        adaptiveLongGroupingPolicy,
                        flatKeyTablePolicy);
            }
            distinctKeySet.reserveAdditional(sourceMask.selectedCount());
            selectedCount = distinctKeySet.addBatch(values, nulls, sourceMask, distinctPositions);
        }
        finally {
            Arrays.fill(values, null);
            Arrays.fill(nulls, null);
        }
        if (selectedCount == sourceMask.selectedCount()) {
            return sourceMask;
        }
        return allocator.allocateSparseMask(allocationContext, distinctPositions, selectedCount, sourceMask.size());
    }

    private static List<TypeBinding> distinctTypes(Schema schema, int[] distinctColumns)
    {
        for (int column : distinctColumns) {
            if (column < 0 || column >= schema.size()) {
                return List.of();
            }
        }
        return Arrays.stream(distinctColumns)
                .mapToObj(column -> schema.field(column).type())
                .toList();
    }

    private final class BatchState
            implements Batch.Lifecycle
    {
        private final Batch sourceBatch;
        private final Mask ownedMask;

        private BatchState(Batch sourceBatch, Mask ownedMask)
        {
            this.sourceBatch = sourceBatch;
            this.ownedMask = ownedMask;
        }

        private Batch sourceBatch()
        {
            return sourceBatch;
        }

        @Override
        public void constrain(Mask mask)
        {
            sourceBatch.constrain(mask);
        }

        @Override
        public Mask takeMask(Mask mask)
        {
            if (mask == ownedMask) {
                allocator.transfer(allocationContext, mask);
            }
            return mask;
        }

        @Override
        public void releaseMask(Mask mask)
        {
            if (mask == ownedMask) {
                allocator.release(allocationContext, mask);
            }
        }

        @Override
        public void close()
        {
            if (currentBatchState == this) {
                currentBatchState = null;
            }
            sourceBatch.close();
        }
    }
}
