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
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.function.Function;

public class MarkDistinctOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("MarkDistinctOperator");

    private final Allocator allocator;
    private final Operator source;
    private final int[] distinctColumns;

    private DistinctKeySet distinctKeySet;
    private int[] distinctPositions = new int[0];
    private BatchState currentBatchState;

    public MarkDistinctOperator(Allocator allocator, int distinctColumn, Operator source)
    {
        this(allocator, new int[] {distinctColumn}, source);
    }

    public MarkDistinctOperator(Allocator allocator, int[] distinctColumns, Operator source)
    {
        this.allocator = allocator;
        this.source = source;
        this.distinctColumns = distinctColumns.clone();
    }

    @Override
    public int outputCount()
    {
        return source.outputCount();
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

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Output sourceOutput = sourceBatch.output(outputIndex);
            outputs[outputIndex] = new Output(
                    sourceOutput.streams(),
                    sourceOutput::borrow,
                    (stream, vector) -> sourceOutput.take(stream),
                    (_, _) -> {},
                    sourceOutput::copySinglePosition);
        }
        return new Batch(
                batchMask,
                batchState::constrain,
                Function.identity(),
                _ -> {},
                () -> {
                    if (currentBatchState == batchState) {
                        currentBatchState = null;
                    }
                    sourceBatch.close();
                },
                outputs);
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
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch().close();
            currentBatchState = null;
        }
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    private Mask computeDistinctMask(Batch sourceBatch, Mask sourceMask)
    {
        if (sourceMask.none()) {
            return sourceMask;
        }

        Vector[] values = new Vector[distinctColumns.length];
        Vector[] nulls = new Vector[distinctColumns.length];
        for (int index = 0; index < distinctColumns.length; index++) {
            Output output = sourceBatch.output(distinctColumns[index]);
            values[index] = output.borrow(Stream.VALUES);
            nulls[index] = output.borrowOrNull(Stream.NULLS);
        }
        if (distinctKeySet == null) {
            distinctKeySet = DistinctKeySet.create(values);
        }
        distinctKeySet.reserveAdditional(sourceMask.selectedCount());

        if (distinctPositions.length < sourceMask.selectedCount()) {
            distinctPositions = new int[sourceMask.selectedCount()];
        }

        int selectedCount = 0;
        if (sourceMask.all()) {
            int size = sourceMask.size();
            for (int position = 0; position < size; position++) {
                if (distinctKeySet.add(values, nulls, position)) {
                    distinctPositions[selectedCount++] = position;
                }
            }
        }
        else {
            for (int position : sourceMask) {
                if (distinctKeySet.add(values, nulls, position)) {
                    distinctPositions[selectedCount++] = position;
                }
            }
        }
        if (selectedCount == sourceMask.selectedCount()) {
            return sourceMask;
        }
        return allocator.allocateSparseMask(ALLOCATION_CONTEXT, distinctPositions, selectedCount, sourceMask.size());
    }

    private record BatchState(Batch sourceBatch, Mask[] maskHolder)
    {
        private BatchState(Batch sourceBatch, Mask mask)
        {
            this(sourceBatch, new Mask[] {mask});
        }

        private void constrain(Mask mask)
        {
            maskHolder[0] = mask;
            sourceBatch.constrain(mask);
        }
    }
}
