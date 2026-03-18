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
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("GroupOperator");
    private final Allocator allocator;

    private final int groupByColumn;
    private final Operator source;
    private final GroupingState groupingState = new GroupingState();
    private BatchState currentBatchState;
    private I64Vector reusableResult;

    public GroupOperator(Allocator allocator, int groupByColumn, Operator source)
    {
        this.allocator = allocator;
        this.groupByColumn = groupByColumn;
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
                outputs[outputIndex] = new Output(sourceOutput.streams(), sourceOutput::borrow, (stream, vector) -> sourceOutput.take(stream));
            }
        }
        return new Batch(batchState.mask, batchState::constrain, ignored -> sourceBatch.takeMask(), outputs);
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
            groupingState.assignGroups(
                    batchState.sourceBatch.output(groupByColumn).borrow(Stream.VALUES),
                    batchState.sourceBatch.output(groupByColumn).borrowOrNull(Stream.NULLS),
                    batchState.mask,
                    batchState.result);
        }
    }

    @Override
    public void close()
    {
        source.close();
        allocator.release(ALLOCATION_CONTEXT);
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
