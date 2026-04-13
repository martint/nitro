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
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

public class GroupOperator
        implements Operator, GroupedKeySource
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GroupOperator");
    private final Allocator allocator;

    private final int[] groupByColumns;
    private final Operator source;
    private final GroupingState groupingState = new GroupingState();
    private BatchState currentBatchState;
    private I64Vector reusableResult;

    public GroupOperator(Allocator allocator, int groupByColumn, Operator source)
    {
        this(allocator, new int[] {groupByColumn}, source);
    }

    public GroupOperator(Allocator allocator, int[] groupByColumns, Operator source)
    {
        this.allocator = allocator;
        this.groupByColumns = groupByColumns.clone();
        this.source = source;
    }

    @Override
    public int outputCount()
    {
        return source.outputCount() + 1;
    }

    @Override
    public Batch next()
    {
        Batch sourceBatch = source.next();
        BatchState batchState = new BatchState(sourceBatch, sourceBatch.borrowMask());
        currentBatchState = batchState;

        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            if (outputIndex == 0) {
                outputs[outputIndex] = new Output(
                        java.util.Set.of(Stream.VALUES),
                        stream -> groupIds(batchState),
                        (stream, vector) -> {
                            if (vector == reusableResult) {
                                reusableResult = null;
                            }
                            batchState.result = null;
                            return allocator.transfer(ALLOCATION_CONTEXT, vector);
                        });
            }
            else {
                Output sourceOutput = sourceBatch.output(outputIndex - 1);
                outputs[outputIndex] = new Output(
                        sourceOutput.streams(),
                        sourceOutput::borrow,
                        (stream, vector) -> sourceOutput.take(stream),
                        (_, _) -> {},
                        sourceOutput::copySinglePosition);
            }
        }
        return new Batch(
                batchState.mask,
                batchState::constrain,
                ignored -> sourceBatch.takeMask(),
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
    public boolean hasNext()
    {
        return source.hasNext();
    }

    @Override
    public void constrain(Mask mask)
    {
        source.constrain(mask);
        if (currentBatchState != null) {
            currentBatchState.constrain(mask);
        }
    }

    private Vector groupIds(BatchState batchState)
    {
        doGroupingIfNeeded(batchState);
        return batchState.result;
    }

    private void doGroupingIfNeeded(BatchState batchState)
    {
        if (!batchState.filled && !batchState.mask.none()) {
            batchState.filled = true;
            reusableResult = allocator.reallocateIfNecessary(ALLOCATION_CONTEXT, reusableResult, I64Vector.class, batchState.mask.maxPosition() + 1, I64Vector::new);
            batchState.result = reusableResult;
            if (groupByColumns.length == 1) {
                int groupByColumn = groupByColumns[0];
                groupingState.assignGroups(
                        batchState.sourceBatch.output(groupByColumn).borrow(Stream.VALUES),
                        batchState.sourceBatch.output(groupByColumn).borrowOrNull(Stream.NULLS),
                        batchState.mask,
                        batchState.result);
            }
            else {
                Vector[] values = new Vector[groupByColumns.length];
                Vector[] nulls = new Vector[groupByColumns.length];
                for (int index = 0; index < groupByColumns.length; index++) {
                    Output output = batchState.sourceBatch.output(groupByColumns[index]);
                    values[index] = output.borrow(Stream.VALUES);
                    nulls[index] = output.borrowOrNull(Stream.NULLS);
                }
                groupingState.assignGroups(values, nulls, batchState.mask, batchState.result);
            }
        }
    }

    @Override
    public void close()
    {
        if (currentBatchState != null) {
            currentBatchState.sourceBatch.close();
            currentBatchState = null;
        }
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
    }

    @Override
    public Streams groupedKeyOutput(int outputIndex, Mask mask, Streams output, Allocator allocator, Allocator.Context allocationContext)
    {
        int groupedKeyIndex = -1;
        for (int index = 0; index < groupByColumns.length; index++) {
            if (outputIndex == groupByColumns[index] + 1) {
                groupedKeyIndex = index;
                break;
            }
        }
        if (groupedKeyIndex == -1) {
            throw new IllegalArgumentException("Output " + outputIndex + " is not a grouping key output");
        }
        return groupingState.groupedValues(groupedKeyIndex, mask, output, allocator, allocationContext);
    }

    private static final class BatchState
    {
        private final Batch sourceBatch;
        private Mask mask;
        private boolean filled;
        private I64Vector result;

        private BatchState(Batch sourceBatch, Mask mask)
        {
            this.sourceBatch = sourceBatch;
            this.mask = mask;
        }

        private void constrain(Mask mask)
        {
            this.mask = mask;
            sourceBatch.constrain(mask);
        }
    }
}
